#TensorFlow

TensorFlow is an open source software library for numerical computation using
data flow graphs.  Nodes in the graph represent mathematical operations, while
the graph edges represent the multidimensional data arrays (tensors) that flow
between them.  This flexible architecture lets you deploy computation to one
or more CPUs or GPUs in a desktop, server, or mobile device without rewriting
code.  TensorFlow was originally developed by researchers and engineers
working on the Google Brain team within Google's Machine Intelligence research
organization for the purposes of conducting machine learning and deep neural
networks research.  The system is general enough to be applicable in a wide
variety of other domains, as well.


**Note: Currently we do not accept pull requests on github -- see
[CONTRIBUTING.md](CONTRIBUTING.md) for information on how to contribute code
changes to TensorFlow through
[tensorflow.googlesource.com](https://tensorflow.googlesource.com/tensorflow)**

**We use [github issues](https://github.com/tensorflow/tensorflow/issues) for
tracking requests and bugs, but please see
[Community](tensorflow/g3doc/resources/index.md#community) for general questions
and discussion.**

# Download and Setup

To install the CPU version of TensorFlow using a binary package, see the
instructions below.  For more detailed installation instructions, including
installing from source, GPU-enabled support, etc., see
[here](tensorflow/g3doc/get_started/os_setup.md).

## Binary Installation

The TensorFlow Python API currently requires Python 2.7: we are
[working](https://github.com/tensorflow/tensorflow/issues/1) on adding support
for Python 3.

The simplest way to install TensorFlow is using
[pip](https://pypi.python.org/pypi/pip) for both Linux and Mac.

For the GPU-enabled version, or if you encounter installation errors, or for
more detailed installation instructions, see
[here](tensorflow/g3doc/get_started/os_setup.md#detailed_install).

### Ubuntu/Linux 64-bit

```bash
# For CPU-only version
$ pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.5.0-cp27-none-linux_x86_64.whl
```

### Mac OS X

```bash
# Only CPU-version is available at the moment.
$ pip install https://storage.googleapis.com/tensorflow/mac/tensorflow-0.5.0-py2-none-any.whl
```

### Try your first TensorFlow program

```sh
$ python

>>> import tensorflow as tf
>>> hello = tf.constant('Hello, TensorFlow!')
>>> sess = tf.Session()
>>> print sess.run(hello)
Hello, TensorFlow!
>>> a = tf.constant(10)
>>> b = tf.constant(32)
>>> print sess.run(a+b)
42
>>>

```

##For more information

* [TensorFlow website](http://tensorflow.org)
* [TensorFlow whitepaper](http://download.tensorflow.org/paper/whitepaper2015.pdf)
=======
TensorFlowTutorials
================

Introduction to deep learning based on Google's TensorFlow framework. These tutorials are direct ports of
Newmu's [Theano Tutorials](https://github.com/Newmu/Theano-Tutorials)

***Topics***
* Simple Multiplication 
* Linear Regression
* Logistic Regression
* Feedforward Neural Network (Multilayer Perceptron)
* Deep Feedforward Neural Network (Multilayer Perceptron with 2 Hidden Layers O.o)
* Convolutional Neural Network

***Dependencies***
* TensorFlow
* Numpy

Theano-Tutorials
================

Bare bones introduction to machine learning from linear regression to convolutional neural networks using Theano.

***Dataset***
It's worth noting that this library assumes that the reader has access to the mnist dataset. This dataset is freely available and is accessible through Yann LeCun's [personal website](http://yann.lecun.com/exdb/mnist/).

If you want to automate the download of the dataset, there is an included file that will do this for you. Simply run the following:
`sudo ./download_mnist.sh`

***Known Issues***
Library not loaded: /usr/local/opt/openssl/lib/libssl.1.0.0.dylib, This results from a broken openssl installation on mac.
It can be fixed by uninstalling and reinstalling openssl: sudo brew remove openssl brew install openssl.
