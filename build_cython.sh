#!/bin/bash
# Compila módulos Cython localmente

echo "Compilando módulos Cython..."
python setup.py build_ext --inplace

echo "Testando importação..."
python -c "from payment_core import FastMemoryDB; print('✓ Cython OK')"