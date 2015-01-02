This datasource is designed to handle multiple hiveserver2 connection from a single datasource, Everytime Datasource.getConnection() is called, it creates a new instance of connection to the underlying hiveserver2. 



