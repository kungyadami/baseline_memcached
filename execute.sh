# # #!/bin/bash
# # export HYDRA_LAUNCHER=ssh
# # export HYDRA_DEBUG=1

# # mpirun \
# #   -hosts konnyang,konduck1 \
# #   -np 1 --wdir /home/mizzz/memcached/memcached ./memcached -v -p 10122 -m 512 -l 117.16.136.217 : \
# #   -np 1 --wdir /home/mizzz/mpi_examples ./memcached_example1

# #!/bin/bash
# export HYDRA_LAUNCHER=ssh
# export HYDRA_DEBUG=1

# # mpirun \
# #   -hosts konnyang,konduck1 \
# #   -np 2 --wdir /home/mizzz/memcached/memcached ./memcached -v -p 10122 -m 512 -l 117.16.136.217 : \
# #   -np 2 --wdir /home/mizzz/mpi_examples bash -c './memcached_example1 > konduck_output.txt'
# #   -np 1 --wdir /home/mizzz/mpi_examples ./memcached_example1 > konduck_output.txt

# mpirun \
#   -hosts konnyang,konduck1 \
#   -np 1 --wdir /home/mizzz/memcached/memcached ./memcached -v -p 10122 -m 512 -l 117.16.136.217 : \
#   -np 2 --wdir /home/mizzz/mpi_examples ./memcached_example1 > /home/mizzz/mpi_examples/konduck_output.txt


#!/bin/bash
export HYDRA_LAUNCHER=ssh
export HYDRA_DEBUG=1

# mpirun \
#   --hostfile hosts.txt \
#   -np 1 --wdir /home/mizzz/memcached/memcached ./memcached -v -p 10122 -m 512 -l 117.16.136.217 : \
#   -np 4 --wdir /home/mizzz/mpi_examples ./memcached_example1 \
#   -np 1 --wdir /home/mizzz/mpi_examples ./memcached_example1 > konduck_output.txt

mpirun \
  --hostfile hosts.txt \
  --bind-to core \
  -np 4 --wdir /home/mizzz/memcached/memcached ./memcached -p 10122 -m 512 -l 117.16.136.217 : \
  -np 4 --wdir /home/mizzz/mpi_examples ./memcached_example1

  #   --map-by slot \