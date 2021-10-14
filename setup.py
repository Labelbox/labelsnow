import setuptools 

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="labelsnow",
    version="0.0.1",
    author="Labelbox",
    author_email="ecosystem+snowflake@labelbox.com",
    description="Labelbox Connector for Snowflake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://labelbox.com",
    packages=setuptools.find_packages(),
    install_requires=[
        "labelbox",
        "pandas",
        "snowflake-connector-python"
    ],
    keywords=["labelbox", "labelsnow"],
)
