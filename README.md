[![Build Status](https://drone.io/github.com/mrkovec/dmcs/status.png)](https://drone.io/github.com/mrkovec/dmcs/latest)
[![Coverage Status](https://coveralls.io/repos/mrkovec/dmcs/badge.svg?branch=master&service=github)](https://coveralls.io/github/mrkovec/dmcs?branch=master)
# dmcs 
prototype

## todo
- [x] basic column store for byte slice data 
- [x] columns of related data stored in a column family structure divided into cache aware blocks
- [x] transaction processing to ensure database determinism
- [ ] lightweight locking (ren et al. 2012)
- [ ] basic query language (scripting)
- [ ] advanced column types
- [ ] rest (rpc)
- [ ] replication, sharding, diagnostic tools
