
-- BF.ADD
BF.ADD 1-1 item1
-- BF.EXISTS 1-1 item1
-- BF.DEBUG 1-1

-- BF.INSERT
-- Add three items to a filter, then create the filter with default parameters if it does not already exist.
BF.INSERT 2-1 ITEMS item1 item2 item3
-- Add one item to a filter, then create the filter with a capacity of 10000 if it does not already exist.
BF.INSERT 2-2 CAPACITY 10000 ITEMS item1
-- Add two items to a filter, then return error if the filter does not already exist.
BF.ADD 2-3 item1
BF.INSERT 2-3 NOCREATE ITEMS item2 item3

-- BF.SCANDUMP

-- BF.LOADCHUNK

-- BF.MADD
BF.MADD 3-1 item1 item2 item3

-- BF.RESERVE
BF.RESERVE 4-1 0.01 1000
BF.RESERVE 4-2 0.01 1000 EXPANSION 2
BF.RESERVE 4-3 0.01 1000 NONSCALING

-- CF.ADD
CF.ADD 5-1 item1
-- CF.DEBUG 5-1

-- CF.ADDNX
CF.ADDNX 6-1 item1

-- CF.INSERT
CF.INSERT 7-1 ITEMS item1 item2 item2
CF.INSERT 7-2 CAPACITY 1000 ITEMS item1 item2 
CF.ADD 7-3 item3
CF.INSERT 7-3 CAPACITY 1000 NOCREATE ITEMS item1 item2 
CF.RESERVE 7-4 2 BUCKETSIZE 1 EXPANSION 0
CF.INSERT 7-4 ITEMS 1 1 1 1

-- CF.INSERTNX
CF.INSERTNX 8-1 CAPACITY 1000 ITEMS item1 item2 
CF.INSERTNX 8-2 CAPACITY 1000 ITEMS item1 item2 item3
CF.ADD 8-3 item3
CF.INSERTNX 8-3 CAPACITY 1000 NOCREATE ITEMS item1 item2 

-- CF.RESERVE 
CF.RESERVE 9-1 1000
CF.RESERVE 9-2 1000 BUCKETSIZE 8 MAXITERATIONS 20 EXPANSION 2

-- CF.SCANDUMP

-- CF.LOADCHUNK 