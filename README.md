Teammates:
Dominick Dupuy
Phillip-Dylan Ndwiga
Hemdutt Rao

Implementation so far:

There are seeds and leeches. The seeds have the data. The leeches want the data. Assume that the seeds are fair with the workload and the leeches will help the seeds send data.

1. All peers connect with each other
2. The seeds break up their file into pieces
3. The seeds divide the work among themselves according to the amount of leeches and other seeds. The division of labor will be fair and if it cannot be fair, seeds with a lower peer_id will get a little more work. 
4. Leechers will wait until data is sent to them.
5. As soon as a leech gets a full data piece, it will act as a seed and distribute data to other leeches
6. Data is continuously propagated until everyone has the data they need.