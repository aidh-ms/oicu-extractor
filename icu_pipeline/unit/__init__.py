# As described in - https://stackoverflow.com/questions/1057431/how-to-load-all-modules-in-a-folder
import glob
from os.path import basename, dirname, isfile, join

from icu_pipeline.unit.converter import BaseConverter, ConverterConfig

# Use BaseConverter::getConverter(source_type, sink_type)

# All available files in this path, which end in .py are possibly modules
modules = glob.glob(join(dirname(__file__), "*.py"))
# Add everything to __all__ if it is a file
__all__ = [basename(f)[:-3] for f in modules if isfile(f) and not f.endswith("__init__.py")]
for m in __all__:
    __import__(f"icu_pipeline.unit.{m}")
    # importlib.import_module(m)
## Necessary in order to use inheritance in abstract class

__all__ += ["BaseConverter", "ConverterConfig"]
