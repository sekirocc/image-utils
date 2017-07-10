Utils to manipulate ceph rbd image.

currently there are two seperate modules. they have no relations.

### rbd.py

this module is used to interact with ceph image.

this module wrap ceph cluster into `Store`, wrap rbd image location into `RbdLocation`,
proxy rbd image in `RbdImageProxy`, read write rbd image through `ImageIterator`

some code may looks familiar to GlanceStore project from openstack.

#### Example: copy a image from source ceph cluster to dest ceph.

```
from rbd_utils import rbd

# get a store.
src_store = rbd.get_store(conffile_path, rbd_user, keyring_path)

# get another store.
dst_store = rbd.get_store(conffile_path2, rbd_user, keyring_path2)

# get a image location from source ceph pool.
src_loc = src_store.get_location(pool, image_name)

# copy image from src_store to dst_store, in dst_pool
rbd.copy(src_store, src_loc, dst_store, dst_pool)

```


### fish.py

this module is used to modify disk image.

#### Example: modify a image.

```
from rbd_utils.fish import Fish

f = Fish(mon_host, mon_port, client, key, pool, image_name)
f.launch()

f.add_mtu('192.168.1.100', 1450)
f.remove_file('/root/abc.txt')

f.shutdown()

```
