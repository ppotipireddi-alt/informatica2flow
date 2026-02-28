# Input
#intermediate_model = {
#   "flow_name": "...",
#   "processors": [...],
#   "connections": [...],
#   "controller_services": [...]
#}
#---------------------------------------------------------



import requests
import logging
from typing import Dict, Any

logger = logging.getLogger("NiFiDeployer")
logging.basicConfig(level=logging.INFO)

class NiFiRestDeployer:

    def __init__(self, nifi_url: str, username=None, password=None):
        self.base_url = nifi_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.username = username
        self.password = password

        if username and password:
            self.session.auth = (username, password)

    # ---------------------------------------------------
    # Helper: Get Revision
    # ---------------------------------------------------
    def get_revision(self, component_uri):
        r = self.session.get(component_uri)
        r.raise_for_status()
        return r.json()["revision"]

    # ---------------------------------------------------
    # Create Process Group
    # ---------------------------------------------------
    def create_process_group(self, parent_group_id, flow_name):

        url = f"{self.base_url}/nifi-api/process-groups/{parent_group_id}/process-groups"

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": flow_name
            }
        }

        r = self.session.post(url, json=payload)
        r.raise_for_status()

        group = r.json()
        logger.info(f"Created process group: {flow_name}")

        return group["id"]

    # ---------------------------------------------------
    # Create Processor
    # ---------------------------------------------------
    def create_processor(self, group_id, processor):

        url = f"{self.base_url}/nifi-api/process-groups/{group_id}/processors"

        payload = {
            "revision": {"version": 0},
            "component": {
                "type": self.resolve_processor_class(processor["type"]),
                "name": processor["name"],
                "config": {
                    "properties": processor["config"]
                }
            }
        }

        r = self.session.post(url, json=payload)
        r.raise_for_status()

        created = r.json()
        logger.info(f"Created processor: {processor['name']}")

        return created["id"]

    # ---------------------------------------------------
    # Create Controller Service
    # ---------------------------------------------------
    def create_controller_service(self, group_id, service):

        url = f"{self.base_url}/nifi-api/process-groups/{group_id}/controller-services"

        payload = {
            "revision": {"version": 0},
            "component": {
                "type": service["type"],
                "name": service["type"],
                "properties": service.get("config", {})
            }
        }

        r = self.session.post(url, json=payload)
        r.raise_for_status()

        created = r.json()
        logger.info(f"Created controller service: {service['type']}")

        return created["id"]

    # ---------------------------------------------------
    # Enable Controller Service
    # ---------------------------------------------------
    def enable_controller_service(self, service_id):

        url = f"{self.base_url}/nifi-api/controller-services/{service_id}"

        revision = self.get_revision(url)

        payload = {
            "revision": revision,
            "component": {
                "id": service_id,
                "state": "ENABLED"
            }
        }

        r = self.session.put(url, json=payload)
        r.raise_for_status()

        logger.info(f"Enabled controller service {service_id}")

    # ---------------------------------------------------
    # Create Connection
    # ---------------------------------------------------
    def create_connection(self, group_id, source_id, target_id):

        url = f"{self.base_url}/nifi-api/process-groups/{group_id}/connections"

        payload = {
            "revision": {"version": 0},
            "component": {
                "source": {"id": source_id},
                "destination": {"id": target_id},
                "selectedRelationships": ["success"]
            }
        }

        r = self.session.post(url, json=payload)
        r.raise_for_status()

        logger.info(f"Created connection {source_id} → {target_id}")

    # ---------------------------------------------------
    # Start Processor
    # ---------------------------------------------------
    def start_processor(self, processor_id):

        url = f"{self.base_url}/nifi-api/processors/{processor_id}"

        revision = self.get_revision(url)

        payload = {
            "revision": revision,
            "component": {
                "id": processor_id,
                "state": "RUNNING"
            }
        }

        r = self.session.put(url, json=payload)
        r.raise_for_status()

        logger.info(f"Started processor {processor_id}")

    # ---------------------------------------------------
    # Deploy Entire Flow
    # ---------------------------------------------------
    def deploy(self, parent_group_id, intermediate_model):

        flow_name = intermediate_model["flow_name"]

        # 1. Create process group
        group_id = self.create_process_group(parent_group_id, flow_name)

        processor_id_map = {}

        # 2. Create controller services
        for service in intermediate_model.get("controller_services", []):
            sid = self.create_controller_service(group_id, service)
            self.enable_controller_service(sid)

        # 3. Create processors
        for processor in intermediate_model["processors"]:
            pid = self.create_processor(group_id, processor)
            processor_id_map[processor["id"]] = pid

        # 4. Create connections
        for conn in intermediate_model["connections"]:
            src = processor_id_map[conn["source"]]
            tgt = processor_id_map[conn["target"]]
            self.create_connection(group_id, src, tgt)

        # 5. Start processors
        for pid in processor_id_map.values():
            self.start_processor(pid)

        logger.info(f"Deployment completed for {flow_name}")

        return group_id

    # ---------------------------------------------------
    # Resolve Processor Class
    # ---------------------------------------------------
    def resolve_processor_class(self, short_name):

        mapping = {
            "QueryRecord": "org.apache.nifi.processors.standard.QueryRecord",
            "LookupRecord": "org.apache.nifi.processors.standard.LookupRecord",
            "RouteOnAttribute": "org.apache.nifi.processors.standard.RouteOnAttribute",
            "UpdateRecord": "org.apache.nifi.processors.standard.UpdateRecord"
        }

        return mapping.get(short_name, short_name)