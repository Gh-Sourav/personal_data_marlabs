class log4j:
    def __init__(self,spark):
        log4j = spark._jvm.org.apache.log4j

        """ This 6,7 line is using to get the appname defined in the spark program"""
        conf = spark.sparkContext.getConf()
        appname = conf.get("spark.app.name")

        """ This 'root_class' is taking my loggar name from 'log4j.properties' file"""
        root_class = "guru.learningjournal.spark.examples"

        self.logger = log4j.LogManager.getLogger(root_class + "." + appname)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, meaasge):
        self.logger.debug(meaasge)