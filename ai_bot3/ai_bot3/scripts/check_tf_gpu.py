#!/usr/bin/env python3
"""Quick TensorFlow GPU sanity check."""
import os
import sys


def main() -> int:
    try:
        import tensorflow as tf
    except Exception as e:
        print(f"ERROR importing tensorflow: {e}")
        return 1

    print(f"tf.version: {tf.__version__}")
    try:
        from tensorflow.python.platform import build_info as tf_build_info
        built_cuda = tf_build_info.build_info.get("is_cuda_build", None)
    except Exception:
        built_cuda = None
    try:
        built_with_cuda = tf.test.is_built_with_cuda()
    except Exception as e:
        built_with_cuda = f"err: {e}"
    print(f"tf.built_with_cuda: {built_with_cuda}")
    print(f"tf.build_info.is_cuda_build: {built_cuda}")

    try:
        phys = tf.config.list_physical_devices()
        phys_gpu = tf.config.list_physical_devices("GPU")
    except Exception as e:
        print(f"ERROR listing physical devices: {e}")
        return 2
    print(f"physical_devices: {phys}")
    print(f"physical_GPUs: {phys_gpu}")

    try:
        logical = tf.config.list_logical_devices()
        logical_gpu = tf.config.list_logical_devices("GPU")
    except Exception as e:
        print(f"ERROR listing logical devices: {e}")
        logical, logical_gpu = [], []
    print(f"logical_devices: {logical}")
    print(f"logical_GPUs: {logical_gpu}")

    if phys_gpu:
        try:
            with tf.device("/GPU:0"):
                a = tf.random.normal((512, 512))
                b = tf.random.normal((512, 512))
                c = tf.matmul(a, b)
                _ = float(tf.reduce_sum(c).numpy())
            print(f"GPU matmul OK: result_shape={c.shape}")
        except Exception as e:
            print(f"GPU matmul FAILED: {e}")
            return 3
    else:
        print("No GPU detected; skipping matmul test.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
