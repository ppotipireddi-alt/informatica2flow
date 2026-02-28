import os
import json
import logging
from typing import Dict, List, Any
from collections import defaultdict, deque
from lxml import etree


# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("InformaticaParser")


# ---------------------------------------------------------
# Helper: Namespace Handling
# ---------------------------------------------------------

def get_namespace_map(root):
    """
    Detect namespace dynamically if present.
    """
    nsmap = root.nsmap.copy()
    nsmap.pop(None, None)  # Remove default None namespace if exists
    return nsmap


def ns_xpath(xpath: str, nsmap: Dict[str, str]):
    """
    Inject namespace prefix if needed.
    """
    if not nsmap:
        return xpath
    prefix = list(nsmap.keys())[0]
    return xpath.replace("//", f"//{prefix}:").replace("/", f"/{prefix}:")


# ---------------------------------------------------------
# Core Parser Class
# ---------------------------------------------------------

class InformaticaXMLParser:

    def __init__(self, xml_path: str):
        self.xml_path = xml_path
        self.tree = None
        self.root = None
        self.nsmap = None

    # -----------------------------------------------------
    # Load XML
    # -----------------------------------------------------
    def load(self):
        try:
            self.tree = etree.parse(self.xml_path)
            self.root = self.tree.getroot()
            self.nsmap = get_namespace_map(self.root)
            logger.info(f"Loaded XML: {self.xml_path}")
        except Exception as e:
            logger.error(f"Failed to load XML: {e}")
            raise

    # -----------------------------------------------------
    # Extract Mapping Name
    # -----------------------------------------------------
    def extract_mapping_name(self):
        mapping = self.root.find(".//MAPPING")
        return mapping.get("NAME") if mapping is not None else None

    # -----------------------------------------------------
    # Extract Sources
    # -----------------------------------------------------
    def extract_sources(self):
        sources = []
        for source in self.root.findall(".//SOURCE"):
            sources.append({
                "name": source.get("NAME"),
                "database_type": source.get("DATABASETYPE"),
                "owner": source.get("OWNER"),
                "fields": [
                    {
                        "name": field.get("NAME"),
                        "datatype": field.get("DATATYPE"),
                        "precision": field.get("PRECISION"),
                        "scale": field.get("SCALE")
                    }
                    for field in source.findall(".//SOURCEFIELD")
                ]
            })
        return sources

    # -----------------------------------------------------
    # Extract Targets
    # -----------------------------------------------------
    def extract_targets(self):
        targets = []
        for target in self.root.findall(".//TARGET"):
            targets.append({
                "name": target.get("NAME"),
                "database_type": target.get("DATABASETYPE"),
                "owner": target.get("OWNER"),
                "fields": [
                    {
                        "name": field.get("NAME"),
                        "datatype": field.get("DATATYPE"),
                        "precision": field.get("PRECISION"),
                        "scale": field.get("SCALE")
                    }
                    for field in target.findall(".//TARGETFIELD")
                ]
            })
        return targets

    # -----------------------------------------------------
    # Extract Transformations
    # -----------------------------------------------------
    def extract_transformations(self):
        transformations = []
        for transformation in self.root.findall(".//TRANSFORMATION"):
            trans_data = {
                "name": transformation.get("NAME"),
                "type": transformation.get("TYPE"),
                "description": transformation.get("DESCRIPTION"),
                "ports": [],
                "attributes": {}
            }

            for port in transformation.findall(".//TRANSFORMFIELD"):
                trans_data["ports"].append({
                    "name": port.get("NAME"),
                    "datatype": port.get("DATATYPE"),
                    "precision": port.get("PRECISION"),
                    "scale": port.get("SCALE"),
                    "port_type": port.get("PORTTYPE"),
                    "expression": port.get("EXPRESSION")
                })

            for attr in transformation.findall(".//TABLEATTRIBUTE"):
                trans_data["attributes"][attr.get("NAME")] = attr.get("VALUE")

            transformations.append(trans_data)

        return transformations

    # -----------------------------------------------------
    # Extract Instances (Mapping-Level Usage)
    # -----------------------------------------------------
    def extract_instances(self):
        instances = []
        for instance in self.root.findall(".//INSTANCE"):
            instances.append({
                "name": instance.get("NAME"),
                "transformation_name": instance.get("TRANSFORMATION_NAME"),
                "type": instance.get("TYPE")
            })
        return instances

    # -----------------------------------------------------
    # Extract Connectors
    # -----------------------------------------------------
    def extract_connectors(self):
        connectors = []
        for connector in self.root.findall(".//CONNECTOR"):
            connectors.append({
                "from_instance": connector.get("FROMINSTANCE"),
                "from_field": connector.get("FROMFIELD"),
                "to_instance": connector.get("TOINSTANCE"),
                "to_field": connector.get("TOFIELD")
            })
        return connectors

    # -----------------------------------------------------
    # Build Execution DAG
    # -----------------------------------------------------
    @staticmethod
    def build_execution_graph(connectors: List[Dict[str, str]]):
        graph = defaultdict(list)
        in_degree = defaultdict(int)

        for conn in connectors:
            src = conn["from_instance"]
            tgt = conn["to_instance"]
            graph[src].append(tgt)
            in_degree[tgt] += 1
            if src not in in_degree:
                in_degree[src] = in_degree[src]

        return graph, in_degree

    # -----------------------------------------------------
    # Topological Sort (Execution Order)
    # -----------------------------------------------------
    @staticmethod
    def topological_sort(graph, in_degree):
        queue = deque([node for node in in_degree if in_degree[node] == 0])
        order = []

        while queue:
            node = queue.popleft()
            order.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order

    # -----------------------------------------------------
    # Complexity Scoring
    # -----------------------------------------------------
    @staticmethod
    def compute_complexity(transformations):
        types = [t["type"] for t in transformations]

        score = 0
        if "Expression" in types:
            score += 1
        if "Lookup Procedure" in types:
            score += 2
        if "Aggregator" in types:
            score += 2
        if "Joiner" in types:
            score += 3
        if "Router" in types:
            score += 1

        if score <= 2:
            return "SIMPLE"
        elif score <= 5:
            return "MEDIUM"
        return "COMPLEX"

    # -----------------------------------------------------
    # Full Parse
    # -----------------------------------------------------
    def parse(self):
        self.load()

        mapping_name = self.extract_mapping_name()
        sources = self.extract_sources()
        targets = self.extract_targets()
        transformations = self.extract_transformations()
        instances = self.extract_instances()
        connectors = self.extract_connectors()

        graph, in_degree = self.build_execution_graph(connectors)
        execution_order = self.topological_sort(graph, in_degree)

        complexity = self.compute_complexity(transformations)

        return {
            "mapping_name": mapping_name,
            "sources": sources,
            "targets": targets,
            "transformations": transformations,
            "instances": instances,
            "connectors": connectors,
            "execution_graph": dict(graph),
            "execution_order": execution_order,
            "complexity": complexity
        }


# ---------------------------------------------------------
# Batch Processing (For 100 Mappings)
# ---------------------------------------------------------

def batch_parse(directory_path: str):
    results = []

    for file in os.listdir(directory_path):
        if file.endswith(".xml"):
            path = os.path.join(directory_path, file)
            parser = InformaticaXMLParser(path)
            try:
                parsed = parser.parse()
                results.append(parsed)
                logger.info(f"Parsed: {file}")
            except Exception as e:
                logger.error(f"Failed parsing {file}: {e}")

    return results


# ---------------------------------------------------------
# Example Usage
# ---------------------------------------------------------

if __name__ == "__main__":
    directory = "input_data"
    parsed_mappings = batch_parse(directory)

    with open("output_data/parsed_output.json", "w") as f:
        json.dump(parsed_mappings, f, indent=2)

    print("Parsing completed successfully.")