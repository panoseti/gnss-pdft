## Environment setup 
```
conda create -n gnss-pdft
conda activate gnss-pdft
conda config --add channels conda-forge
conda config --set channel_priority strict
conda install pyubx2 pyserial pygnssutils grpcio grpcio-tools libgrpc grpcio-reflection rich redis-py jupyter pandas numpy 
```
