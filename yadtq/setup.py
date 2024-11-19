from setuptools import setup, find_packages

setup(
    name="yadtq",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # Add your project dependencies here, for example:
        # 'redis',
        # 'rabbitmq-server',
        # etc.
    ],
    author="droq",
    author_email="droq@yahoo.com",
    description="yadtq",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    keywords="task queue, distributed",
    url="",  # If you have a repository URL
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
)
