from connections_manager import AirflowConnectionsManager


connections = AirflowConnectionsManager.list_connections()
print(connections)