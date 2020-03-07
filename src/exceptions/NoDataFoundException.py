class NoDataFoundException(Exception):

	def __init__(self,message=None):
		if message is None:
			self.message = "We just didn't find any data..."
		else:
			self.message = message