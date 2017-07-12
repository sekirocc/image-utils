import guestfs


class Fish(object):
    def __init__(self, mon_host, mon_port, client, key, pool, image_name):
        g = guestfs.GuestFS(python_return_dict=True)

        rbd_image = "%s/%s" % (pool, image_name)
        mon_server = "%s:%s" % (mon_host, mon_port)
        g.add_drive_opts(rbd_image, format="raw", protocol="rbd",
                         server=[mon_server], username=client, secret=key)
        self.g = g

    def launch(self):
        self.g.launch()

        roots = self.g.inspect_os()
        root = roots[0]

        mps = self.g.inspect_get_mountpoints(root)
        # example mps:
        # {
        #    '/': '/dev/mapper/VGSYS',
        #    '/boot': '/dev/sda1',
        #    '/data3': '/dev/vdd'
        # }
        assert '/' in mps, ('root path / is not in mountpoints! '
                            'can not modify this image!')

        # mount with read & write
        self.g.mount(mps['/'], '/')

    def add_mtu(self, ip, mtu):
        """
        for interface with the given ip, add MTU=mtu line to its network script
        """
        path = '/etc/sysconfig/network-scripts'
        scripts = self.g.ls(path)

        for script in scripts:
            filename = path + '/' + script
            lines = self.g.head_n(100, filename)

            found = False
            for line in lines:
                if ip in line:
                    # this is our script.
                    found = True
                    break

            if found:
                lines.append("MTU=" + str(mtu))
                self.g.write(filename, '\n'.join(lines))
                return

        print('network-script with ip(%s) config is not found' % ip)

    def remove_file(self, filepath):
        """
        """
        self.g.rm_rf(filepath)

    def shutdown(self):
        self.g.umount_all()
        self.g.close()
