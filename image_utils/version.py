from pbr import version as pbr_version

loaded = False
version_info = pbr_version.VersionInfo('image-utils')
version_string = version_info.version_string
