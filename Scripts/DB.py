import pyodbc

class DB:
	def __init__(self):
		try:
			connstr = "DSN=NZSQL;DATABASE=<Database_name>"
			self.conn =  pyodbc.connect(connstr)
			self.cur = self.conn.cursor()
		except:
			print "I can't connect to the database"
	def cursor(self):
		return self.cur
	def getNewCursor(self):
		return self.conn.cursor()
	def connection(self):
		return self.conn

if __name__ == '__main__':
        db = DB()
        cur = db.cursor()
        #cur.execute("SELECT * from v_clin_doc limit 10;")
        #print cur.fetchall()
	print "connected!"
