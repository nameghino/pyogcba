import ckanclient, urllib2, helpers
import logging, csv

logging.basicConfig(level=logging.DEBUG)

def get_ckan():
	my_key = ''
	gcba_ckan_location = 'http://data.buenosaires.gob.ar/api'
	return ckanclient.CkanClient(api_key=my_key, base_location=gcba_ckan_location)

class DatasetParser:
	def __init__(self, dataset, resource):
		logging.debug("opening %s" % resource['id'])
		self.file = open(resource['id'], 'rU')
		self.data = {}
		if self.file != None:
			logging.debug("file is open and ready")


	def parse(self):
		pass


class CSVParser(DatasetParser):
	def __init__(self, dataset, resource):
	   DatasetParser.__init__(self, dataset, resource)
	   self.separator = dataset.get_separator()
	   self.dict_reader = csv.DictReader(self.file, delimiter=self.separator, quotechar='"')

	def parse(self):
		table = []
		keys = None
		header = True
		for row in self.dict_reader:
			table.append(row)
			print row
		return table

class Dataset:
	def __init__(self, ckan_object, package_name):
		self.data = None
		self.ckan_ref = ckan_object
		self.raw_metadata = None
		self.package_name = package_name
		self.identifier = None
	
	def get_separator(self):
		ugly = self.raw_metadata['extras']['Delimitador'].lower()
		if ugly == 'coma':
			return ','
		elif ugly == 'punto y coma':
			return ';'


	def is_loaded(self):
		return self.data != None

	def query(self, dataset_id, filter_fn=lambda x: True):
		if (!self.is_loaded()):
			self.load()
		resource = self.get_resource(dataset_id)
		resource_key = self.get_resource_key(resource)
		results = []
		for row in self.data[resource_key]:
			if filter_fn(row) == True:
				results.append(row)
		return results

	def get_resource(self, key):
		for resource in self.raw_metadata['resources']:
			if key == resource['name']:
				return resource
			if key == resource['id']:
				return resource
			if key == resource['url'].split("/")[-1].split(".")[0]:
				return resource
		return None

	def get_resource_key(self, resource):
		return (resource['id'], resource['url'].split("/")[-1].split(".")[0])

	def get_available_datasets(self):
		return [self.get_resource(k[0]) for k in self.data.keys()]

	def get_available_dataset_keys(self):
		return [k for k in self.data.keys()]	

	def load(self):
		logging.debug("loading dataset metadata")
		self.raw_metadata = self.ckan_ref.package_entity_get(self.package_name)
		logging.debug("done. found %d resources" % len(self.raw_metadata['resources']))
		for resource in self.raw_metadata['resources']:
			logging.debug("resource data: %s" % resource)
			logging.debug("looking for parser for %s format" % resource['format'])


			parser_class = None
			try:
				parser_class = helpers.get_class("opendata_gcba." + resource['format'] + "Parser")
				if parser_class is None:
					logging.error("parser not found. going on with next resource")
					continue
			except:
				logging.error("parser not found. going on with next resource")
				continue
			

			download_required = False
			try:
				fh = open(resource['id'], 'rU')
			except IOError as e:
				download_required = True
			logging.debug("parser found, moving on")
			
			if download_required == True:
				target_url = resource['url']
				dataset_file = urllib2.urlopen(target_url)
				output = open(resource['id'], 'wb')
				output.write(dataset_file.read())
				output.close()

			logging.debug("written %s" % resource['id'])
			logging.debug("parsing")
			parser = parser_class(self, resource)
			
			if self.data == None:
				self.data = {}

			data = parser.parse()
			k = self.get_resource_key(resource)
			logging.debug("self.data[%s] --> %s (%d)" % (resource['id'], type(data), len(data)))
			self.data[k] = data

			logging.debug("%d records found" % len(self.data[self.get_resource_key(resource)]))