"""Gconfiglib module variables."""
from typing import Optional

from kazoo.client import KazooClient

# Internal module variables. Values are assigned at runtime
# Zookeeper connection (if using zookeeper)
zk_conn: Optional[KazooClient] = None
# Zookeeper update flag (used for managing update collisions in triggers)
zk_update: bool = False
