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


# #!/bin/bash
# export HYDRA_LAUNCHER=ssh
# export HYDRA_DEBUG=1

# # mpirun \
# #   --hostfile hosts.txt \
# #   -np 1 --wdir /home/mizzz/memcached/memcached ./memcached -v -p 10122 -m 512 -l 117.16.136.217 : \
# #   -np 4 --wdir /home/mizzz/mpi_examples ./memcached_example1 \
# #   -np 1 --wdir /home/mizzz/mpi_examples ./memcached_example1 > konduck_output.txt

# mpirun \
#   --hostfile hosts.txt \
#   --bind-to core \
#   -np 4 --wdir /home/mizzz/memcached/memcached ./memcached -p 10122 -m 512 -l 117.16.136.217 : \
#   -np 4 --wdir /home/mizzz/mpi_examples ./memcached_example1

#   #   --map-by slot \

#!/bin/bash
# export HYDRA_LAUNCHER=ssh
# export HYDRA_DEBUG=1

# MPIRUN=$HOME/mvapich-3.0/install/bin/mpirun
# LIBS="$HOME/mvapich-3.0/install/lib:$HOME/mvapich-3.0/install/mvapich/lib"

# $MPIRUN \
#   --hostfile hosts.txt \
#   -np 1 --wdir /home/mizzz/Memcached \
#     -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
#     -genv LD_LIBRARY_PATH "$LIBS" \
#     -genv UCX_TLS rc \
#     -genv UCX_NET_DEVICES mlx5_0:1 \
#     -genv UCX_LOG_LEVEL info \
#     ./memcached -p 10122 -m 512 -l 117.16.136.217 : \
#   -np 1 --wdir /home/mizzz/mpi_examples \
#     -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
#     -genv LD_LIBRARY_PATH "$LIBS" \
#     -genv UCX_TLS rc \
#     -genv UCX_NET_DEVICES mlx5_0:1 \
#     -genv UCX_LOG_LEVEL info \
#     ./memcached_example1

#!/usr/bin/env bash
# set -euo pipefail

# MPIRUN="$HOME/mvapich-3.0/install/bin/mpirun"
# LIBS="$HOME/mvapich-3.0/install/lib:$HOME/mvapich-3.0/install/mvapich/lib"

# "$MPIRUN" \
#   -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
#   --hostfile hosts.txt \
#   -genv LD_LIBRARY_PATH "$LIBS" \
#   -genv UCX_TLS rc \
#   -genv UCX_NET_DEVICES mlx5_0:1 \
#   -genv UCX_LOG_LEVEL info \
#   -l -print-all-exitcodes \
#   \
#   -np 1 --wdir "$HOME/Memcached"     ./memcached -p 10122 -m 512 -l 117.16.136.217 : \
#  -np 1 --wdir "$HOME/mpi_examples"  ./memcached_example1

# MPIRUN=$HOME/mvapich-3.0/install/bin/mpirun
# LIBS="$HOME/mvapich-3.0/install/lib:$HOME/mvapich-3.0/install/mvapich/lib:$HOME/libmemcached-1.0.18/libmemcached/.libs"

# # $MPIRUN -hosts KonGoose,konduck1 -np 2 \
# #   -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
# #   -genv LD_LIBRARY_PATH "$LIBS" \
# #   /bin/sh -lc 'echo "$(hostname)  LD_LIBRARY_PATH=$LD_LIBRARY_PATH"'

# $MPIRUN -hosts KonGoose -np 1 \
#   -genv LD_LIBRARY_PATH "$LIBS" \
#   /bin/sh -lc 'ldd ~/Memcached/memcached | egrep "libmemcached|not found"'

# $MPIRUN -hosts konduck1 -np 1 \
#   -genv LD_LIBRARY_PATH "$LIBS" \
#   /bin/sh -lc 'ldd $HOME/mpi_examples/memcached_example1 | egrep "libmemcached|not found"'


# MPIRUN=$HOME/mvapich-3.0/install/bin/mpirun
# LIBS="$HOME/mvapich-3.0/install/lib:$HOME/mvapich-3.0/install/mvapich/lib:$HOME/libmemcached-1.0.18/libmemcached/.libs"

# "$MPIRUN" \
#   -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
#   --hostfile hosts.txt \
#   -genv LD_LIBRARY_PATH "$LIBS" \
#   -genv UCX_TLS rc \
#   -genv UCX_NET_DEVICES mlx5_0:1 \
#   -genv UCX_LOG_LEVEL error \
#   -l -print-all-exitcodes \
#   \
#   -np 1 --wdir "$HOME/parameter_server" python3 parameter_server.py : \
#   -np 1 --wdir "$HOME/Memcached" ./memcached -p 10122 -m 512 -l 114.70.21.66 : \
#   -np 1 --wdir "$HOME/Worker" python3 worker.py



MPIRUN=$HOME/mvapich-3.0/install/bin/mpirun
LIBS="$HOME/mvapich-3.0/install/lib:$HOME/mvapich-3.0/install/mvapich/lib:$HOME/libmemcached-1.0.18/libmemcached/.libs"

"$MPIRUN" \
  -bootstrap ssh -bootstrap-exec /usr/bin/ssh \
  --hostfile hosts.txt \
  -genv LD_LIBRARY_PATH "$LIBS" \
  -genv UCX_TLS rc \
  -genv UCX_NET_DEVICES mlx5_0:1 \
  -genv UCX_LOG_LEVEL error \
  -l -print-all-exitcodes \
  \
  -np 16 --wdir "$HOME/Memcached" ./memcached -p 10122 -m 4096 -l 114.70.21.66 : \
  -np 16 --wdir "$HOME/mpi_examples" ./memcached_example1
