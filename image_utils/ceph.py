import time
import rbd
import rados
import math
from six.moves import urllib
import six
import progressbar

from oslo_utils import units

from image_utils import errors
from image_utils.logger import logger


DEFAULT_CHUNK_SIZE = 32  # MB
READ_CHUNK_MULTIPLE = 8  # read 256 MB at at time
DEFAULT_TIMEOUT = 10     # in seconds


def get_store(conffile, rbd_user, keyring):
    conf = CephConf(conffile, rbd_user, keyring)
    return Store(conf)


def get_location(store, pool, image, snapshot=None):
    return RbdLocation({
        'fsid': store.get_fsid(pool),
        'pool': pool,
        'image': image,
        'snapshot': snapshot or '',
    })


def copy(src_store, src_loc, dst_store, dst_pool):
    """
    copy image from source ceph to destination ceph pool
    """
    start = time.time()
    image_file, image_size = src_store.get_image(src_loc)
    print('start copy image, ')
    print('first create image(%s) in dst pool(%s)' % (
          image_file.name, dst_pool))

    try:
        dst_loc = dst_store.create(dst_pool, image_file.name, image_size)
    except errors.Duplicate:
        print('found existing image! so use it.')
        dst_loc = dst_store.get_location(dst_pool, image_file.name)

    print('copying...')
    with RbdImageProxy(dst_store,
                       dst_loc.pool,
                       dst_loc.image, None) as dst_image:
        offset = 0

        pbar_widgets = [progressbar.Bar('=', '[', ']'), ' ',
                        progressbar.Percentage()]
        pbar = progressbar.ProgressBar(maxval=image_size, widgets=pbar_widgets)
        pbar.start()

        for data in image_file:
            length = len(data)
            try:
                dst_image.write(data, offset)
            except Exception:
                logger.info('Failed to store image %s to dst store ' %
                            dst_image.name)
                raise

            offset += length
            pbar.update(offset)

        pbar.finish()

    use_time = time.time() - start

    print('finish copy image(%s) to pool(%s), size: %dMB, time %ds\n' %
          (image_file.name, dst_pool, image_size / 1024 / 1024, use_time))

    logger.info('copy image OK, use time: %ds' % use_time)


class RbdImageProxy(object):

    def __init__(self, store, pool, name, snapshot=None):
        self.store = store
        self.pool = pool
        self.name = name
        if snapshot:
            self.snapshot = snapshot
        else:
            self.snapshot = None

    def __enter__(self):
        client, ioctx = self.store.connect(self.pool)
        try:
            image = rbd.Image(ioctx, self.name, snapshot=self.snapshot)
        except rbd.ImageNotFound:
            msg = "RBD image %s does not exist" % self.name
            logger.info(msg)
            raise errors.NotFound(message=msg)

        self.client = client
        self.ioctx = ioctx
        self.image = image

        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        try:
            self.image.close()
        finally:
            self.store.disconnect(self.client, self.ioctx)

        if not ex_value:
            return True
        raise

    def __getattr__(self, attr):
        if not hasattr(self, 'image'):
            raise AttributeError("You should enter context before access attr")

        return getattr(self.image, attr)


class Store(object):

    def __init__(self, ceph_conf):
        self.ceph_conf = ceph_conf
        # pool to operate on.
        self.pool = None
        self._fsid = None

    @property
    def chunk_size(self):
        return self.ceph_conf.chunk_size

    @property
    def connect_timeout(self):
        return self.ceph_conf.connect_timeout

    @property
    def rbd_ceph_conf(self):
        return self.ceph_conf.rbd_ceph_conf

    @property
    def rbd_user(self):
        return self.ceph_conf.rbd_user

    @property
    def rbd_keyring(self):
        return self.ceph_conf.rbd_keyring

    def connect(self, pool):
        client = rados.Rados(conffile=self.rbd_ceph_conf,
                             rados_id=self.rbd_user,
                             conf=dict(keyring=self.rbd_keyring))
        try:
            client.connect(timeout=self.connect_timeout)
        except rados.Error as ex:
            logger.info(ex)
            msg = "Error connecting to ceph cluster."
            raise errors.CanNotConnect(msg)

        ioctx = client.open_ioctx(pool)
        return client, ioctx

    def disconnect(self, client, ioctx):
        try:
            ioctx.close()
            client.shutdown()
        except:
            logger.info('disconnect from rados failed.')

    def get_location(self, pool, name):
        return RbdLocation({
            'fsid': self.get_fsid(pool),
            'pool': pool,
            'image': name,
            'snapshot': None,
        })

    def get_image(self, loc):
        """
        return a file-like handler
        """

        image_file = ImageIterator(loc.pool, loc.image, loc.snapshot, self)
        image_size = image_file.size

        return (image_file, image_size)

    def get_fsid(self, pool):
        if not self._fsid:
            client, ioctx = self.connect(pool)
            try:
                fsid = client.get_fsid()
            except:
                fsid = None
            finally:
                self.disconnect(client, ioctx)

            self._fsid = fsid

        return self._fsid

    def create(self, pool, name, image_size):
        """
        create a image in store, return the location.
        """
        client, ioctx = self.connect(pool)

        order = int(math.log(self.chunk_size, 2))
        logger.info('Creating image %s with order %d and size %d' %
                    (name, order, image_size))

        try:
            librbd = rbd.RBD()
            features = client.conf_get('rbd_default_features')
            if (features is None or int(features) == 0):
                features = rbd.RBD_FEATURE_LAYERING

            librbd.create(ioctx,
                          name,
                          image_size,
                          order,
                          old_format=False,
                          features=int(features))
            logger.info('Create image %s successfully' % name)

            try:
                fsid = client.get_fsid()
            except:
                fsid = None
            return RbdLocation({
                'fsid': fsid,
                'pool': pool,
                'image': name,
                'snapshot': None,
            })

        except rbd.ImageExists:
            msg = 'RBD image %s already exists' % name
            logger.info(msg)
            raise errors.Duplicate(message=msg)

        finally:
            self.disconnect(client, ioctx)

    def delete(self, pool, name, snapshot=None):
        # First remove snapshot in image context.
        if snapshot:
            try:
                self.delete_snap(pool, name, snapshot)

            except errors.NotFound:
                pass

            except:
                raise

        # Then delete image in io context.
        try:
            client, ioctx = self.connect(pool)

            try:
                rbd.RBD().remove(ioctx, name)
            except rbd.ImageHasSnapshots:
                msg = 'Image %s has snapshots. can not delete' % name
                logger.info(msg)
                raise errors.HasSnapshot(msg)

            except rbd.ImageBusy:
                msg = 'Image %s is in use.' % name
                logger.info(msg)
                raise errors.ImageIsInUse(msg)

            except rbd.ImageNotFound:
                msg = "RBD image %s does not exist" % name
                logger.info(msg)
                # raise errors.NotFound(msg)

            logger.info('Delete snapshot %s for %s successfully' %
                        (snapshot, name))

        except errors.NotFound:
            pass

        except:
            raise

        finally:
            self.disconnect(client, ioctx)

    def delete_snap(self, pool, name, snapshot):
        logger.info('Deleting snapshot %s for image %s.' % (snapshot, name))
        with RbdImageProxy(self, pool, name, None) as image:
            try:
                image.unprotect_snap(snapshot)
                image.remove_snap(snapshot)

            except rbd.ImageNotFound:
                msg = "Snapshot %s does not exist" % snapshot
                logger.info(msg)
                # raise errors.NotFound(msg)

            except rbd.ImageBusy:
                msg = 'Snapshot %s is in ues.' % snapshot
                logger.info(msg)
                raise errors.ImageIsInUse(msg)

        logger.info('Delete snapshot %s for %s successfully' %
                    (snapshot, name))

    def has_snap(self, pool, name, snapshot):
        logger.info('Checking image %s has snapshot %s.' % (name, snapshot))
        with RbdImageProxy(self, pool, name, None) as image:
            for snap in image.list_snaps():
                logger.debug('\t snapshot: %s' % snap)
                if snapshot == snap['name']:
                    logger.info('\t has found: %s!' % snapshot)
                    return True
        return False

    def snap(self, pool, name, snapshot):
        logger.info('Creating snapshot %s for image %s.' % (snapshot, name))
        with RbdImageProxy(self, pool, name, None) as image:
            image.create_snap(snapshot)
            image.protect_snap(snapshot)

        logger.info('Create snapshot: %s for %s successfully' %
                    (snapshot, name))

        return RbdLocation({
            'fsid': self.get_fsid(pool),
            'pool': pool,
            'image': name,
            'snapshot': snapshot,
        })

    def rename(self, pool, name, new_name):
        logger.info('Renaming image %s to %s' % (name, new_name))

        try:
            client, ioctx = self.connect(pool)
            rbd.RBD().rename(ioctx, str(name), str(new_name))
        except:
            raise
        else:
            logger.info('Rename successfully')
        finally:
            self.disconnect(client, ioctx)


class ImageIterator(object):
    """
    Reads data from an RBD image, one chunk at a time.
    """
    def __init__(self, pool, name, snapshot, store):
        self.pool = pool
        self.name = name
        self.snapshot = snapshot

        # read 256 MB at a time.
        self.read_chunk_size = store.chunk_size * READ_CHUNK_MULTIPLE

        self.store = store
        self._size = -1

    def __iter__(self):
        try:
            with RbdImageProxy(self.store, self.pool, self.name, self.snapshot) as image:   # noqa
                size = left = image.size()

                while left > 0:
                    length = min(self.read_chunk_size, left)
                    data = image.read(size - left, length)
                    left -= len(data)
                    yield data

                raise StopIteration()

        except rbd.ImageNotFound:
            msg = 'RBD image %s does not exist' % self.name
            logger.info(msg)
            raise errors.NotFound(message=msg)  # noqa

    @property
    def size(self):
        if self._size <= 0:
            try:
                with RbdImageProxy(self.store,
                                   self.pool,
                                   self.name,
                                   self.snapshot) as image:

                    self._size = image.size()

            except rbd.ImageNotFound:
                logger.info('RBD image %s does not exist' % self.name)
                raise errors.NotFound(message=msg)  # noqa

        return self._size


def get_location_from_uri(uri):
    loc = RbdLocation()
    loc.parse_uri(uri)
    return loc


class RbdLocation(object):
    """
    Class describing a RBD URI. This is of the form:
        rbd://fsid/pool/image/snapshot
    """

    def __init__(self, specs=None):
        self.specs = specs
        if self.specs:
            self.process_specs()

    def process_specs(self):
        # convert to ascii since librbd doesn't handle unicode
        for key, value in six.iteritems(self.specs):
            self.specs[key] = str(value)
        self.fsid = self.specs.get('fsid')
        self.pool = self.specs.get('pool')
        self.image = self.specs.get('image')
        self.snapshot = self.specs.get('snapshot')

    def get_uri(self):
        # ensure nothing contains / or any other url-unsafe character
        safe_fsid = urllib.parse.quote(self.fsid, '')
        safe_pool = urllib.parse.quote(self.pool, '')
        safe_image = urllib.parse.quote(self.image, '')
        safe_snapshot = urllib.parse.quote(self.snapshot, '')
        return "rbd://%s/%s/%s/%s" % (safe_fsid, safe_pool,
                                      safe_image, safe_snapshot)

    def parse_uri(self, uri):
        prefix = 'rbd://'
        if not uri.startswith(prefix):
            msg = "Invalid URI: %s. URI must start with rbd://" % uri
            logger.info(msg)
            raise errors.BadRbdUri(message=msg)

        # convert to ascii since librbd doesn't handle unicode
        try:
            ascii_uri = str(uri)
        except UnicodeError:
            msg = "Invalid URI: %s. URI contains non-ascii characters" % uri
            logger.info(msg)
            raise errors.BadRbdUri(message=msg)

        pieces = ascii_uri[len(prefix):].split('/')
        if len(pieces) == 4:
            self.fsid, self.pool, self.image, self.snapshot = \
                map(urllib.parse.unquote, pieces)
        else:
            msg = "Invalid URI: %s. URI must have exactly 4 components" % uri    # noqa
            logger.info(msg)
            raise errors.BadRbdUri(message=msg)

        if any(map(lambda p: p == '', pieces)):
            msg = "Invalid URI: %s. URI cannot contain empty components" % uri
            logger.info(msg)
            raise errors.BadRbdUri(message=msg)


class CephConf(object):

    def __init__(self, rbd_ceph_conf, rbd_user, rbd_keyring,
                 connect_timeout=None, chunk_size=None):

        self.rbd_ceph_conf = rbd_ceph_conf
        self.rbd_user = rbd_user
        self.rbd_keyring = rbd_keyring
        self.connect_timeout = connect_timeout or DEFAULT_TIMEOUT
        self.chunk_size = (chunk_size or DEFAULT_CHUNK_SIZE) * units.Mi   # MB
