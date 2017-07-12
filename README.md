Utils to manipulate ceph ceph image.

currently there are two seperate modules. they have no relations.

### Install

```
pip install image-utils
```

### Usage

currently there are two modules.

#### ceph.py

this module is used to interact with ceph image.

##### Example: copy a image from source ceph cluster to dest ceph.

```
from ceph_utils import ceph

# get source store.
src_store = ceph.get_store(conffile_path, ceph_user, keyring_path)

# get dest store.
dst_store = ceph.get_store(conffile_path2, ceph_user, keyring_path2)

# get an image location from source ceph pool.
src_loc = src_store.get_location(pool, image_name)

# copy image from src_store to dst_store, in dst_pool
ceph.copy(src_store, src_loc, dst_store, dst_pool)

```

#### fish.py

this module is used to modify disk image.

##### Example: modify a image.

```
from ceph_utils.fish import Fish

f = Fish(mon_host, mon_port, client, key, pool, image_name)
f.launch()

f.add_mtu('192.168.1.100', 1450)
f.remove_file('/root/abc.txt')

f.shutdown()

```
