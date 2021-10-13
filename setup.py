from setuptools import setup


setup(
   name="rabbit_helper",
   version="0.3.0",
   description="A helper for RabbitMQ which allows for protocol versioning",
   author='Blue',
   url="https://nqn.blue/",
   packages=["rabbit_helper"],
   install_requires=["aio-pika"]
)
