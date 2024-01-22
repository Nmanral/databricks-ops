import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="workflow",
    version="0.1.0",
    author="Nitin Manral",
    author_email="",
    description="A simple workflow package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Nmanral11/databricks-ops",
    license='MIT',
    packages=['workflow'],
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=['boto3',
                      'requests']
)