from mpi4py import MPI

comm=MPI.COMM_WORLD

rank = comm.rank
size = comm.size

if rank==0:
    data_shared= (rank+1)*5
    comm.send(data_shared,dest=(rank*2)%size)
	
else:
    data=comm.recv(source=0)
    """ comment the above statement uncomment the next statement if
        multiple sources send data"""
    #data=comm.recv(source=(rank-3)%size)
    print data
    print 'rank:',rank

