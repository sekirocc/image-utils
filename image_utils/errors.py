import six


class BaseException(Exception):
    message = "An unknown exception occurred"

    def __init__(self, message=None, **kwargs):
        if not message:
            message = self.message
        try:
            if kwargs:
                message = message % kwargs
        except Exception:
            pass

        self.msg = message
        super(BaseException, self).__init__(message)

    def __unicode__(self):
        # NOTE(flwang): By default, self.msg is an instance of Message, which
        # can't be converted by str(). Based on the definition of
        # __unicode__, it should return unicode always.
        return six.text_type(self.msg)


class CanNotConnect(BaseException):
    message = "Ceph backend connect error"


class BadRbdUri(BaseException):
    message = "The Rbd URI was malformed: %(uri)s"


class Duplicate(BaseException):
    message = "Image %(image)s already exists"


class NotFound(BaseException):
    message = "Image %(image)s not found"


class ImageIsInUse(BaseException):
    message = "Image %(image)s is in use"


class HasSnapshot(BaseException):
    message = "Image %(image)s has snapshot."


class CephNotRegistered(BaseException):
    message = "Ceph %(ceph)s is not registered."
