

# import faiss                   # make faiss available
# index = faiss.IndexFlatL2(d)   # build the index
# print(index.is_trained)
# index.add(xb)                  # add vectors to the index
# print(index.ntotal)
#
#
#
# k = 4                          # we want to see 4 nearest neighbors
# D, I = index.search(xb[:5], k) # sanity check
# print(I)
# print(D)
# D, I = index.search(xq, k)     # actual search
# print(I[:5])                   # neighbors of the 5 first queries
# print(I[-5:])                  # neighbors of the 5 last queries