# Intermediate Model Structure
# ---------------------------------------------------------
# {
#   "flow_name": "m_customer",
#   "processors": [
#       {
#           "id": "node_1",
#           "type": "QueryRecord",
#           "name": "Expression_Conversion",
#           "config": {...}
#       }
#   ],
#   "connections": [
#       {
#           "source": "node_1",
#           "target": "node_2"
#       }
#   ],
#   "controller_services": []
# }
#---------------------------------------------------------  


import uuid
import logging
from typing import Dict, List, Any


logger = logging.getLogger("NiFiIntermediateConverter")


# ------------------------------------------------------------
# Utility
# ------------------------------------------------------------

def generate_id():
    return str(uuid.uuid4())


# ------------------------------------------------------------
# Core Converter
# ------------------------------------------------------------

class NiFiIntermediateConverter:

    def __init__(self, parsed_mapping: Dict[str, Any], rule_engine=None):
        self.mapping = parsed_mapping
        self.rule_engine = rule_engine  # MCP rule memory hook
        self.processors = []
        self.connections = []
        self.controller_services = []
        self.instance_to_node = {}

    # --------------------------------------------------------
    # Entry Point
    # --------------------------------------------------------
    def convert(self):
        logger.info(f"Converting mapping: {self.mapping['mapping_name']}")

        execution_order = self.mapping.get("execution_order", [])
        transformations = {t["name"]: t for t in self.mapping["transformations"]}

        # Convert transformations in execution order
        for instance_name in execution_order:
            transformation = transformations.get(instance_name)

            if not transformation:
                continue

            node = self.convert_transformation(transformation)
            self.instance_to_node[instance_name] = node["id"]
            self.processors.append(node)

        # Build connections
        self.build_connections()

        return {
            "flow_name": self.mapping["mapping_name"],
            "processors": self.processors,
            "connections": self.connections,
            "controller_services": self.controller_services
        }

    # --------------------------------------------------------
    # Transformation Routing
    # --------------------------------------------------------
    def convert_transformation(self, transformation):

        t_type = transformation["type"]

        if t_type == "Expression":
            return self.convert_expression(transformation)

        elif t_type == "Lookup Procedure":
            return self.convert_lookup(transformation)

        elif t_type == "Aggregator":
            return self.convert_aggregator(transformation)

        elif t_type == "Router":
            return self.convert_router(transformation)

        elif t_type == "Joiner":
            return self.convert_joiner(transformation)

        else:
            return self.generic_passthrough(transformation)

    # --------------------------------------------------------
    # Expression → QueryRecord
    # --------------------------------------------------------
    def convert_expression(self, transformation):

        sql_expressions = []

        for port in transformation["ports"]:
            if port["expression"]:
                expr = port["expression"]

                # Apply rule engine if available
                if self.rule_engine:
                    expr = self.rule_engine.apply(expr)

                sql_expressions.append(
                    f"{expr} AS {port['name']}"
                )

        sql_query = "SELECT " + ", ".join(sql_expressions) + " FROM FLOWFILE"

        return {
            "id": generate_id(),
            "type": "QueryRecord",
            "name": transformation["name"],
            "config": {
                "sql": sql_query
            }
        }

    # --------------------------------------------------------
    # Lookup → LookupRecord
    # --------------------------------------------------------
    def convert_lookup(self, transformation):

        service_id = generate_id()

        controller_service = {
            "id": service_id,
            "type": "DBLookupService",
            "config": {}
        }

        self.controller_services.append(controller_service)

        return {
            "id": generate_id(),
            "type": "LookupRecord",
            "name": transformation["name"],
            "config": {
                "lookup_service": service_id
            }
        }

    # --------------------------------------------------------
    # Aggregator → QueryRecord
    # --------------------------------------------------------
    def convert_aggregator(self, transformation):

        group_by = []
        aggregates = []

        for port in transformation["ports"]:
            if port["port_type"] == "GROUPBY":
                group_by.append(port["name"])
            elif port["expression"]:
                aggregates.append(
                    f"{port['expression']} AS {port['name']}"
                )

        sql = "SELECT "
        sql += ", ".join(group_by + aggregates)
        sql += " FROM FLOWFILE"

        if group_by:
            sql += " GROUP BY " + ", ".join(group_by)

        return {
            "id": generate_id(),
            "type": "QueryRecord",
            "name": transformation["name"],
            "config": {"sql": sql}
        }

    # --------------------------------------------------------
    # Router → RouteOnAttribute
    # --------------------------------------------------------
    def convert_router(self, transformation):

        routes = {}

        for port in transformation["ports"]:
            if port["expression"]:
                routes[port["name"]] = port["expression"]

        return {
            "id": generate_id(),
            "type": "RouteOnAttribute",
            "name": transformation["name"],
            "config": {
                "routes": routes
            }
        }

    # --------------------------------------------------------
    # Joiner → QueryRecord (SQL JOIN)
    # --------------------------------------------------------
    def convert_joiner(self, transformation):

        join_condition = transformation["attributes"].get("Join Condition", "1=1")

        sql = f"SELECT * FROM FLOWFILE LEFT JOIN other_stream ON {join_condition}"

        return {
            "id": generate_id(),
            "type": "QueryRecord",
            "name": transformation["name"],
            "config": {"sql": sql}
        }

    # --------------------------------------------------------
    # Generic Fallback
    # --------------------------------------------------------
    def generic_passthrough(self, transformation):
        return {
            "id": generate_id(),
            "type": "UpdateRecord",
            "name": transformation["name"],
            "config": {}
        }

    # --------------------------------------------------------
    # Build Connections
    # --------------------------------------------------------
    def build_connections(self):

        for connector in self.mapping["connectors"]:

            src = connector["from_instance"]
            tgt = connector["to_instance"]

            if src in self.instance_to_node and tgt in self.instance_to_node:

                self.connections.append({
                    "source": self.instance_to_node[src],
                    "target": self.instance_to_node[tgt]
                })