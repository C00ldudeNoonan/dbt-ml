"""Built-in transforms users reference from YAML by their dotted module path:

    transform:
      type: python
      module: docbt.text.transforms.text_stats
      options:
        text_field: body

These are batteries-included for common text preprocessing. Users can override
any of them by writing their own `transforms/<name>.py` (project-local files
win over installed packages — see docbt.transforms.runner.load_transform).
"""
