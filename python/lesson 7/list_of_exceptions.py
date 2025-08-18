import builtins

exceptions = [name for name in dir(builtins) 
              if isinstance(getattr(builtins, name), type) 
              and issubclass(getattr(builtins, name), 
                             BaseException)]
print("\n".join(exceptions))