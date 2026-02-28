"""
nifi_direct_deployer.py
-----------------------
Deploys a NiFi flow JSON (produced by NiFiFlowGenerator) directly via the
NiFi REST API — no XML templates involved.

Steps:
  1. Authenticate → get JWT token
  2. Create a Process Group with the mapping name
  3. Create Controller Services inside the PG
  4. Create Processors inside the PG
  5. Create Connections between processors
"""

import json
import logging
import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("NiFiDirectDeployer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class NiFiDirectDeployer:

    def __init__(self, nifi_url: str, username: str, password: str):
        self.base = nifi_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({"Content-Type": "application/json"})
        self._login(username, password)

    def _login(self, username: str, password: str):
        r = self.session.post(
            f"{self.base}/nifi-api/access/token",
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        self.session.headers.update({"Authorization": f"Bearer {r.text.strip()}"})
        logger.info("✅ Authenticated with NiFi")

    def _post(self, path: str, body: dict) -> dict:
        r = self.session.post(f"{self.base}{path}", json=body)
        if not r.ok:
            logger.error(f"POST {path} failed {r.status_code}: {r.text[:500]}")
            r.raise_for_status()
        return r.json()

    # ── 1. Process Group ──────────────────────────────────────────────────────

    def create_process_group(self, parent_id: str, name: str, x: float = 200, y: float = 200) -> str:
        body = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": x, "y": y},
            },
        }
        data = self._post(f"/nifi-api/process-groups/{parent_id}/process-groups", body)
        pg_id = data["id"]
        logger.info(f"✅ Process Group created: {name} ({pg_id})")
        return pg_id

    # ── 2. Controller Services ────────────────────────────────────────────────

    def create_controller_service(self, pg_id: str, svc: dict) -> str:
        """Create a controller service inside a process group."""
        properties = {}
        for k, v in svc.get("config", {}).items():
            properties[k] = str(v)

        body = {
            "revision": {"version": 0},
            "component": {
                "name": svc.get("name", svc["type"]),
                "type": svc.get("nifi_class", svc["type"]),
                "properties": properties,
            },
        }
        data = self._post(f"/nifi-api/process-groups/{pg_id}/controller-services", body)
        svc_id = data["id"]
        logger.info(f"  ↳ Controller Service: {svc.get('name')} ({svc_id})")
        return svc_id

    # ── 3. Processors ─────────────────────────────────────────────────────────

    def create_processor(self, pg_id: str, proc: dict, svc_id_map: dict) -> str:
        """Create a processor inside a process group, remapping service IDs."""
        properties = {}
        for k, v in proc.get("config", {}).items():
            if k == "routes":
                continue
            # Remap old controller service IDs to new ones
            if isinstance(v, str) and v in svc_id_map:
                v = svc_id_map[v]
            properties[k] = str(v) if v is not None else ""

        position = proc.get("position", {"x": 100, "y": 100})

        body = {
            "revision": {"version": 0},
            "component": {
                "name": proc["name"],
                "type": proc.get("nifi_class", proc["type"]),
                "position": {"x": float(position.get("x", 100)), "y": float(position.get("y", 100))},
                "config": {
                    "schedulingStrategy": proc.get("scheduling", {}).get("schedulingStrategy", "TIMER_DRIVEN"),
                    "schedulingPeriod": proc.get("scheduling", {}).get("schedulingPeriod", "0 sec"),
                    "concurrentlySchedulableTaskCount": 1,
                    # Use pre-computed list from flow generator (only unused rels)
                    "autoTerminatedRelationships": proc.get(
                        "auto_terminate_relationships",
                        proc.get("relationships", [])   # fallback for older JSON
                    ),
                    "properties": properties,
                },
            },
        }
        data = self._post(f"/nifi-api/process-groups/{pg_id}/processors", body)
        proc_id = data["id"]
        logger.info(f"  ↳ Processor: [{proc['type']:25s}] {proc['name']} ({proc_id})")
        return proc_id

    # ── 4. Connections ────────────────────────────────────────────────────────

    def create_connection(self, pg_id: str, source_id: str, dest_id: str, relationships: list) -> str:
        body = {
            "revision": {"version": 0},
            "component": {
                "source": {"id": source_id, "groupId": pg_id, "type": "PROCESSOR"},
                "destination": {"id": dest_id, "groupId": pg_id, "type": "PROCESSOR"},
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
            },
        }
        data = self._post(f"/nifi-api/process-groups/{pg_id}/connections", body)
        return data["id"]

    # ── Main Deploy ───────────────────────────────────────────────────────────

    def _patch_auto_terminate(self, proc_new_id: str, all_rels: list, used_rels: set):
        """Patch a processor to auto-terminate only relationships not used by a connection."""
        unused = [r for r in all_rels if r not in used_rels]
        if not unused:
            return
        # Get current revision
        r = self.session.get(f"{self.base}/nifi-api/processors/{proc_new_id}")
        r.raise_for_status()
        data = r.json()
        revision = data["revision"]
        component = data["component"]
        component["config"]["autoTerminatedRelationships"] = unused
        body = {"revision": revision, "component": component}
        r2 = self.session.put(f"{self.base}/nifi-api/processors/{proc_new_id}", json=body)
        if r2.ok:
            logger.info(f"  ↳ Auto-terminated unused rels {unused} on {component['name']}")
        else:
            logger.warning(f"  ↳ Could not patch auto-terminate on {proc_new_id}: {r2.text[:200]}")

    def deploy_flow_json(self, flow_json_path: str, parent_pg_id: str = "root") -> str:
        """
        Read a JSON flow file (from NiFiFlowGenerator) and deploy it to NiFi.
        Returns the new process group ID.

        Two-phase deployment:
          Phase 1 — create processors with no auto-terminated relationships
          Phase 2 — create connections, then patch each processor to
                    auto-terminate only relationships NOT used by a connection
                    (avoids the dotted-line artefact in the NiFi UI).
        """
        with open(flow_json_path) as f:
            flow = json.load(f)

        flow_name = flow["flow_name"]
        logger.info(f"Deploying flow: {flow_name}")

        # Phase 1a — Process group
        pg_id = self.create_process_group(parent_pg_id, flow_name)

        # Phase 1b — Controller services
        svc_id_map = {}
        for svc in flow.get("controller_services", []):
            old_id = svc["id"]
            svc_id_map[old_id] = self.create_controller_service(pg_id, svc)

        # Phase 1c — Processors (no auto-terminate yet)
        proc_id_map = {}
        proc_rels_map = {}   # old_id → all relationship names
        for proc in flow.get("processors", []):
            old_id = proc["id"]
            proc_id_map[old_id] = self.create_processor(pg_id, proc, svc_id_map)
            proc_rels_map[old_id] = proc.get("relationships", [])

        # Phase 2a — Connections; track which relationships are used per source
        used_rels_by_old_id: dict = {k: set() for k in proc_id_map}
        conn_count = 0
        for conn in flow.get("connections", []):
            src_old = conn["source_id"]
            dst_old = conn["target_id"]
            src_new = proc_id_map.get(src_old)
            dst_new = proc_id_map.get(dst_old)
            if src_new and dst_new:
                rels = conn.get("selected_relationships", ["success"])
                self.create_connection(pg_id, src_new, dst_new, rels)
                used_rels_by_old_id[src_old].update(rels)
                conn_count += 1
            else:
                logger.warning(f"Skipping connection: {src_old} → {dst_old}")

        # Phase 2b — Patch processors: auto-terminate only unused relationships
        for old_id, new_id in proc_id_map.items():
            all_rels = proc_rels_map.get(old_id, [])
            used_rels = used_rels_by_old_id.get(old_id, set())
            self._patch_auto_terminate(new_id, all_rels, used_rels)

        logger.info(f"\n✅ Deployed '{flow_name}' to NiFi:")
        logger.info(f"   Process Group ID : {pg_id}")
        logger.info(f"   Processors       : {len(proc_id_map)}")
        logger.info(f"   Connections      : {conn_count}")
        logger.info(f"   Controller Svcs  : {len(svc_id_map)}")
        logger.info(f"\n   Open in NiFi UI  : https://localhost:8443/nifi")
        return pg_id


# ─────────────────────────────────────────────────────────────────────────────
# Direct run
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import NIFI_CONFIG

    flow_json = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "output_data",
        "M_POL_WKD_FILE_COSTCENTER_nifi_flow.json"
    )

    deployer = NiFiDirectDeployer(
        NIFI_CONFIG["url"],
        NIFI_CONFIG["username"],
        NIFI_CONFIG["password"],
    )
    deployer.deploy_flow_json(flow_json, parent_pg_id="root")
