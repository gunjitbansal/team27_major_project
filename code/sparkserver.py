import cherrypy

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType


sc = SparkContext()
sparkServerContext = SQLContext(sc)

class SparkServer(object):
	exposed = True

	@cherrypy.tools.accept(media='text/plain')
	def GET(self):
		pass

	def POST(self, url55, schemastring, table, query):
		lines = sc.textFile(url55)
		print "Here"
		parts = lines.map(lambda l: l.split(","))
		content = parts.map(lambda p: tuple(p))
		fields = [StructField(field_name, StringType(), True) for field_name in schemastring.split(",")]
		schema = StructType(fields)

		# Apply the schema to the RDD.
		print "0"
		schemaTable = sparkServerContext.applySchema(content, schema)
		print "1--------------------------------------------------------------------------"
		schemaTable.registerTempTable(table)
		print "2---------------------------------------------------------------------------"
		result = sparkServerContext.sql(query)
		print "3"
		finalresult=result.map(lambda row: ",".join(row))
		print "4"
		output=""
		k=finalresult.collect()
		flag = False
		for record in k:
			if flag == True:
				output = output + "\n" + record
			else:
				output = record
				flag = True
		
		print "output "
		print output
		return output

	def PUT(self):
		pass

	def DELETE(self):
		pass

if __name__ == '__main__':
	conf = {
	'/': {
	'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
	'tools.sessions.on': True,
	'tools.response_headers.on': True,
	'tools.response_headers.headers': [('Content-Type', 'text/plain')],
	}
	}
	cherrypy.config.update({
                         'server.socket_port': 5555,
                        }) 
	cherrypy.quickstart(SparkServer(), '/', conf)
