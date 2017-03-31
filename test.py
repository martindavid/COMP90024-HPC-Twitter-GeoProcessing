# import math
# from mpi4py import MPI

# comm = MPI.COMM_WORLD
# rank = comm.Get_rank()

# if rank == 0:
#     data = [4, 9, 16, 25]

# else:
#     data = None

# val = comm.scatter(data, root=0)
# if rank > 0:
#     print('%d - %d' % (rank, val))

# results = comm.gather(math.sqrt(val), root=0)





from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0: 
    data = [(x+1) ** x for x in range (size)]
    print('scattering data',data)
else:
    data = None


data = comm.scatter(data,root=0)
print('rank',rank,'has data: ', data)
    
new_data = comm.gather(data, root=0)
if rank == 0:
    print('master collected: ', new_data)