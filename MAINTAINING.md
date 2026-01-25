# Maintaining S3Lib

This guide provides information for maintainers and contributors working on S3Lib.

## Table of Contents

- [Development Setup](#development-setup)
- [Code Structure](#code-structure)
- [Running Tests](#running-tests)
- [Dependencies](#dependencies)
- [Adding New Features](#adding-new-features)
- [Release Process](#release-process)
- [Common Tasks](#common-tasks)
- [Troubleshooting](#troubleshooting)

## Development Setup

### Prerequisites

- Python 3.7 or higher
- pip
- virtualenv or venv (recommended)

### Setting Up Development Environment

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd s3lib
   ```

2. Create a virtual environment:
   ```bash
   python3 -m venv env3
   source env3/bin/activate  # On Windows: env3\Scripts\activate
   ```

3. Install in development mode:
   ```bash
   pip install -e .
   ```

4. Install development dependencies:
   ```bash
   pip install tox pytest pytest-cov
   ```

5. Set up test credentials (for manual testing):
   ```bash
   # Create ~/.s3 with your test AWS credentials
   echo "YOUR_ACCESS_KEY_ID" > ~/.s3
   echo "YOUR_SECRET_ACCESS_KEY" >> ~/.s3
   ```

## Code Structure

The project is organized as follows:

```
s3lib/
├── s3lib/               # Main package directory
│   ├── __init__.py      # Core S3 library (Connection class, S3 API)
│   ├── ui.py            # Command-line interface implementations
│   └── utils.py         # Utility functions (batching, signing, parsing)
├── tests/               # Test suite
│   ├── test__init__.py  # Tests for core library
│   ├── test_ui.py       # Tests for CLI utilities
│   └── test_utils.py    # Tests for utility functions
├── setup.py             # Package configuration and entry points
├── tox.ini              # Test automation configuration
├── README.md            # User documentation
└── MAINTAINING.md       # This file
```

### Core Components

#### s3lib/__init__.py

This file contains the core S3 library functionality:

- **Connection class**: Main interface for S3 operations
  - Context manager support (`__enter__`, `__exit__`)
  - HTTP connection management (`_connect`, `_disconnect`)
  - High-level operations: `list_buckets`, `list_bucket`, `get_object`, `put_object`, `delete_object`, etc.
  - Low-level S3 request handling: `_s3_request`, `_s3_get_request`, etc.
  - AWS signature version 2 authentication

- **Helper functions**:
  - `sign()`: Create HMAC-SHA1 signature for authentication
  - `sign_content()`: Create MD5 hash for content verification
  - XML parsing functions for S3 responses

**Design decisions**:
- Uses HTTP (not HTTPS) by default for performance and simplicity
- Implements AWS Signature Version 2 (older but simpler than v4)
- Memory-efficient streaming via generators and chunked reads
- Fixed-memory operations for large objects

#### s3lib/ui.py

Command-line interface implementations using docopt for argument parsing:

- Each utility has a `*_main()` function that serves as entry point
- Credential loading with fallback chain: file → env vars → ~/.s3
- Uses `safeoutput` library for safe file writing with atomic operations
- Streaming copy operations for memory efficiency

**Key patterns**:
- All functions accept `argv=None` parameter for testability
- Use `docopt` for consistent CLI parsing
- Error handling with meaningful messages
- Support for stdin/stdout streaming

#### s3lib/utils.py

Utility functions used across the library:

- **Iteration helpers**: `take()`, `batchify()` for memory-efficient batching
- **Header handling**: `split_headers()` separates AWS-specific headers
- **Signing utilities**: `get_string_to_sign()` builds canonical strings for signing
- **Error handling**: `raise_http_resp_error()` for consistent error reporting

### Entry Points

The package defines command-line tools via setuptools entry points in `setup.py`:

```python
entry_points = {
  'console_scripts': [
    's3ls   = s3lib.ui:ls_main',
    's3get  = s3lib.ui:get_main',
    's3cp   = s3lib.ui:cp_main',
    's3head = s3lib.ui:head_main',
    's3put  = s3lib.ui:put_main',
    's3rm   = s3lib.ui:rm_main',
    's3sign = s3lib.ui:sign_main',
  ],
}
```

## Running Tests

### Using Tox (Recommended)

Tox runs tests across multiple Python versions:

```bash
# Run all tests across all configured Python versions
tox

# Run tests for specific Python version
tox -e py37
tox -e py39

# Run linting checks
tox -e lint

# Recreate test environments (useful after dependency changes)
tox -r
```

### Using Pytest Directly

For faster iteration during development:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_utils.py

# Run with coverage report
pytest tests/ --cov=s3lib --cov-report=html

# Run specific test
pytest tests/test_utils.py::test_batchify
```

### Test Coverage

View coverage reports:

```bash
# Generate HTML coverage report
tox

# Open report in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Dependencies

### Runtime Dependencies

Defined in `setup.py`:

- **safeoutput>=2.0**: Provides atomic file writing operations
  - Used in CLI tools to safely write output files
  - Prevents partial writes on errors

- **future**: Python 2/3 compatibility library
  - Note: Python 2 support was dropped in recent versions
  - Can likely be removed in future cleanup

- **docopt**: Declarative command-line argument parsing
  - Allows defining CLI via docstrings
  - Simpler than argparse for straightforward CLIs

### Development Dependencies

- **tox**: Test automation across Python versions
- **pytest>=7.2.2**: Testing framework
- **pytest-cov**: Coverage plugin for pytest
- **coverage**: Code coverage measurement
- **yapf**: Code formatting (for linting)
- **isort**: Import sorting (for linting)

### Dependency Philosophy

- Minimal runtime dependencies for easier installation
- Standard library preferred when possible
- Avoid heavy dependencies (boto3) to keep package lightweight
- Development dependencies isolated via tox

## Adding New Features

### Adding a New S3 Operation

1. **Add method to Connection class** (`s3lib/__init__.py`):

   ```python
   def new_operation(self, bucket, key, **kwargs):
       """Your operation description."""
       # Add high-level operation
       status, headers = self._s3_new_request(bucket, key)
       return (status, headers)

   def _s3_new_request(self, bucket, key):
       """Low-level HTTP request for operation."""
       resp = self._s3_request("GET", bucket, key, {}, {}, '')
       if resp.status != http.client.OK:
           raise_http_resp_error(resp)
       return (resp.status, resp.getheaders())
   ```

2. **Add tests** (`tests/test__init__.py`):

   ```python
   def test_new_operation():
       # Test your new operation
       pass
   ```

3. **Update documentation** (`README.md`):
   - Add API documentation
   - Add usage example

### Adding a New CLI Utility

1. **Define usage string** (`s3lib/ui.py`):

   ```python
   NEWCMD_USAGE = """
   s3newcmd -- Description

   Usage:
       s3newcmd [options] <bucket> <args>...

   Options:
       --host=<host>   Custom S3 endpoint
       --creds=<path>  Credentials file path
   """
   ```

2. **Implement main function** (`s3lib/ui.py`):

   ```python
   def newcmd_main(argv=None):
       args = docopt(NEWCMD_USAGE, argv)
       (access_id, secret_key) = load_creds(args.get('--creds'))
       with Connection(access_id, secret_key, args.get('--host')) as s3:
           # Implement command logic
           result = s3.new_operation(args.get('<bucket>'), ...)
           print(result)
   ```

3. **Register entry point** (`setup.py`):

   ```python
   entry_points = {
     'console_scripts': [
       # ... existing commands ...
       's3newcmd = s3lib.ui:newcmd_main',
     ],
   }
   ```

4. **Add tests** (`tests/test_ui.py`):

   ```python
   def test_newcmd_main():
       # Test CLI behavior
       pass
   ```

5. **Update documentation** (`README.md`):
   - Add command to utilities section
   - Add usage examples

### Code Style Guidelines

- Follow PEP 8 style guide
- Use 4 spaces for indentation
- Maximum line length: 100 characters (flexible)
- Use meaningful variable names
- Add docstrings to public functions
- Include type hints in docstrings where helpful

Example:

```python
def example_function(param1, param2):
    """
    Brief description of function.

    param1 is str
    param2 is int
    returns bool
    """
    # Implementation
    return True
```

## Release Process

### Version Numbering

S3Lib follows semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality, backwards compatible
- **PATCH**: Bug fixes, backwards compatible

Current version is defined in `setup.py`.

### Creating a Release

1. **Update version** in `setup.py`:

   ```python
   setup(
       name='S3Lib',
       version='2.1.0',  # Update this
       # ...
   )
   ```

2. **Update CHANGELOG** (create if doesn't exist):
   - Document all changes since last release
   - Categorize: Added, Changed, Fixed, Deprecated, Removed

3. **Run full test suite**:

   ```bash
   tox
   ```

4. **Commit version bump**:

   ```bash
   git add setup.py CHANGELOG.md
   git commit -m "Bump version to 2.1.0"
   git tag v2.1.0
   ```

5. **Build distribution packages**:

   ```bash
   python setup.py sdist bdist_wheel
   ```

6. **Upload to PyPI**:

   ```bash
   # Test PyPI first (recommended)
   twine upload --repository testpypi dist/*

   # Production PyPI
   twine upload dist/*
   ```

7. **Push to repository**:

   ```bash
   git push origin master
   git push origin v2.1.0
   ```

### Pre-release Checklist

- [ ] All tests passing
- [ ] Code coverage acceptable (check htmlcov/index.html)
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Version bumped in setup.py
- [ ] No debug code or print statements
- [ ] Dependencies up to date

## Common Tasks

### Updating Dependencies

1. Update version in `setup.py`:

   ```python
   requires = ['safeoutput>=3.0', 'future', 'docopt']
   ```

2. Recreate tox environments:

   ```bash
   tox -r
   ```

3. Test thoroughly across Python versions

### Adding Python Version Support

1. Update `setup.py` classifiers:

   ```python
   classifiers=[
       # ...
       'Programming Language :: Python :: 3.10',  # Add new version
   ]
   ```

2. Update `tox.ini`:

   ```ini
   [tox]
   envlist = py{37,39,310}  # Add new version
   ```

3. Test with new Python version:

   ```bash
   tox -e py310
   ```

### Debugging S3 Issues

Enable HTTP debugging:

```python
import http.client
http.client.HTTPConnection.debuglevel = 1

# Run your code
```

This will print all HTTP traffic, useful for debugging signature issues or API problems.

### Checking Signature Issues

The library uses AWS Signature Version 2. If seeing authentication errors:

1. Verify credentials are correct
2. Check system time (signatures are time-sensitive)
3. Enable HTTP debugging to see canonical string
4. Compare with AWS documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html

## Troubleshooting

### Tests Failing

**Problem**: Tests fail with credential errors

**Solution**: Tests should not require real AWS credentials. Check if tests are properly mocked. Add mocking if needed.

**Problem**: Tests fail on specific Python version

**Solution**: Check for version-specific code. Use `sys.version_info` checks or update code for compatibility.

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 's3lib'`

**Solution**: Install in development mode:
```bash
pip install -e .
```

### Tox Errors

**Problem**: `InterpreterNotFound: pythonX.Y`

**Solution**: Install missing Python version or remove from tox.ini envlist

**Problem**: Tox using cached/old code

**Solution**: Recreate environments:
```bash
tox -r
```

### Performance Issues

**Problem**: Large file operations consuming too much memory

**Solution**: Ensure streaming operations are used. Check that code uses chunked reads/writes:

```python
# Good - streaming
chunk = response.read(65536)
while chunk:
    process(chunk)
    chunk = response.read(65536)

# Bad - loads entire file
data = response.read()  # Loads everything into memory
```

**Problem**: List operations timing out on large buckets

**Solution**: Use batching parameters:
```bash
s3ls mybucket --batch 100  # Smaller batches
```

## Contact and Support

- **Author**: Andrew Thomson (athomsonguy@gmail.com)
- **Repository**: Check setup.py for URL
- **Issues**: Report bugs via GitHub issues

## Future Improvements

Potential areas for enhancement:

1. **AWS Signature Version 4**: Upgrade from v2 for better security
2. **HTTPS by default**: Secure connections out of the box
3. **Async support**: Add async/await for concurrent operations
4. **Multipart uploads**: Support for large files >5GB
5. **Progress indicators**: Show progress for long operations
6. **Better error messages**: More descriptive S3 error handling
7. **Type hints**: Full PEP 484 type annotations
8. **Remove future dependency**: Clean up Python 2 compatibility code

When implementing these, maintain backwards compatibility where possible.
