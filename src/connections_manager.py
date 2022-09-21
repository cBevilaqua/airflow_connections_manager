from api_client import AirflowApiClient


class AirflowConnectionsManager:
    api_client = AirflowApiClient()

    @staticmethod
    def list_connections():
        conns = None
        try:
            conns = AirflowConnectionsManager.api_client.list_connections()
            conns = conns.json()
        except Exception as error:
            return error

        output = []
        connections = conns["connections"]

        if connections is not None and len(connections) > 0:
            for item in connections:
                output.append({"connection_id": item["connection_id"], "value": "*****"})
        return output

    @staticmethod
    def get_connection(name):
        try:
            conn = AirflowConnectionsManager.api_client.get_connection(name)
        except Exception as error:
            return error

        conn = conn.json()

        if "status" in conn and conn["status"] == 404:
            return "Secret not found!"

        output = None

        if conn["extra"] is not None:
            return conn["extra"]
        else:
            output = {}
            output["host"] = conn["host"]
            output["port"] = conn["port"]
            output["database"] = conn["schema"]
            output["username"] = conn["login"]
        return output

    @staticmethod
    def delete_connection(name):
        try:
            AirflowConnectionsManager.api_client.delete_connection(name)
        except Exception as error:
            return error
        return f"Successfully deleted secret with id {name}"

    @staticmethod
    def create_db_connection(name, host, port, database, username, password):
        data = {
            "connectionId": name,
            "host": host,
            "port": int(port),
            "database": database,
            "username": username,
            "password": password,
        }
        try:
            AirflowConnectionsManager.api_client.create_db_connection(data)
        except Exception as error:
            return error
        return f"Successfully created database connection with id {name}"

    @staticmethod
    def create_generic_connection(name, data: dict):
        input_data = {"connectionId": name, "extra": data}
        try:
            AirflowConnectionsManager.api_client.create_generic_connection(input_data)
        except Exception as error:
            return error
        return f"Successfully created generic connection with id {name}"
