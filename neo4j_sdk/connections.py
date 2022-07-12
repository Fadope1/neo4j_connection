from typing import Optional, Union

from dataclasses import dataclass
from neo4j import GraphDatabase


@dataclass
class Node:
    type: str
    properties: dict


@dataclass
class Relation:
    base_node: Node
    relationship: str
    final_node: Node
    properties: Optional[dict] = None


class Database:
    """
    Neo4j SDK for adding/ updating Nodes/ Relationships.
    Commands maybe helpfull:
    - :config
    - :sysinfo
    - :COMMAND
    """

    def __init__(self, url, user, pw):
        """Connect to the database."""
        self.driver = GraphDatabase.driver(url, auth=(user, pw))
        self.session = self.driver.session()

        self.query: str = "" # pending query

    def __enter__(self):
        """Run on context manager enter."""
        return self

    def clear(self):
        """Clears the neo4j database."""
        query = "MATCH (N) DETACH DELETE N"
        self.session.write_transaction(self._run, query)

    def _add_properties(self, properties: dict):
        self.query += "{"
        for key, value in properties.items():
            self.query += f"{key}: '{value}'\n"
        self.query = self.query.rstrip('\n')
        self.query += "}"

    def create_node(self, node: Node):
        """Create a new node -> append to pending query."""
        if self.query: # add newline if not empty
            self.query += "\n"

        self.query += "CREATE ("

        if node.properties.get('name', None):
            self.query += node.properties['name']
        self.query += f":{node.type}"

        if node.properties:
            self._add_properties(node.properties)

        self.query += ")"

    def create_relationhip(self, node1, relation, node2):
        if self.query: # add newline if not empty
            self.query += "\n"

        self.query += "CREATE ("

        self.query += f"({node1})-[:{relation}"
        # if relation.properties:
        #     self._add_properties(relation.properties)

        self.query += f"]->({node2}))"

    def old_create_relationhip(self, relation: Relation):
        """Create a new relationship between pending nodes -> append to pending query."""
        if self.query: # add newline if not empty
            self.query += "\n"

        """CREATE (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix)"""

        self.query += "CREATE ("

        self.query += f"({relation.base_node})-[:{relation.relationship}"
        if relation.properties:
            self._add_properties(relation.properties)

        self.query += f"]->({relation.final_node}))"

    def commit(self):
        """Commit the pending query."""
        self.session.write_transaction(self._run, self.query)
        self.query = "" # reset query

    @staticmethod
    def _run(tx, query):
        tx.run(query)

    def add_relation(self, relation: Relation):
        """Search for a existing relationship and update it."""
        self.query = Database.match("base_node", relation.base_node)+"\n"
        self.query += Database.match("final_node", relation.final_node)+"\n"

        self.query += f"CREATE "

        self.query += f"(base_node)-[:{relation.relationship}"

        if relation.properties:
            self._add_properties(relation.properties)

        self.query += "]->(final_node)"

    def update_node(self, find: Node, replace: Node):
        """Search for a existing node and update it."""
        self.query = Database.match("base", find)+"\n"
        for key, value in replace.properties.items():
            self.query += f"SET base.{key} = '{value}'\n"

        self.query += "RETURN base"

    @staticmethod
    def match_by_id(match_by: str, id: Union[str, int]) -> str:
        """Return node by id."""
        return f"MATCH ({match_by}) WHERE ID({match_by}) = {id}"

    @staticmethod
    def match(match_by: str, node: Node) -> str:
        """Search a node by args."""

        query = f"MATCH ({match_by}"

        if node.type:
            query += f":{node.type}"

        query += "{"
        for key, value in node.properties.items():
            query += f"{key}: '{value}',"
        query = query.rstrip(",")
        query += "})"

        return query

    def generate_grass_file(self, level: int, node: Node):
        """Change the size of a node type."""
        raise NotImplementedError("Coming soon... Hopefully... Maybe...")

    def close(self):
        """Close the connection."""
        self.session.close()
        self.driver.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Run on context manager exit."""
        self.close()


if __name__ == "__main__":
    url = "bolt://127.0.0.1:7687"
    user = "neo4j"
    password = "123"


    with Database(url, user, password) as session:
        session.update_node(Node("Stock", {'name': 'BASF test'}), Node("Stock", {'name': 'BASF new', 'isin': 'DE12121212'}))
        session.commit()
        # session.create_node(Node(Journalist", "{name: 'Spiegel'}"))
        # session.create_node(Node("Stock", "{name: 'BASF SE', isin: 'DE12121212'}"))
        # session.create_node(Node("News", "{date: '2020', headline: 'BASF wins', content: 'Basf is winning because...'}"))
        # session.create_relationhip(Relation("BASF_is_winning", "POSITIVE", "basf"))
        # session.create_relationhip(Relation("spiegel", "WRITTEN", "BASF_is_winning"))
        # session.commit()
        # print(session.get_node({'name': 'BASF SE'}))
        # print(session.get_node_by_id(34))
        # print(session.add_relation(Relation(base_node=Node("Journalist", {'name': 'Spiegel'}), relationship="LIKES", final_node=Node("Stock", {'name': 'BASF SE'}))))
        # print(session.add_relation(Relation(base_node=Node("Stock", {'name': 'BASF SE'}), relationship="HATES", final_node=Node("Journalist", {'name': 'Spiegel'}))))
