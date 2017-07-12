import logging

log_file = '/tmp/image-utils.log'
fmt = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s'

logging.basicConfig(filename=log_file,
                    filemode='a',
                    format=fmt,
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('image')
logger.setLevel(logging.DEBUG)
