from typing import List,Dict,Callable,TypeVar

Input = TypeVar("Input")

Key = TypeVar("Key")

def group_by(objects:List[Input],\
             group_key_func:Callable[[Input],Key])->Dict[Key,List[Input]]:
    
    grouped:Dict[Key,List[Input]] = dict()

    for obj in objects:
        group_key = group_key_func(obj)

        if group_key not in grouped:
            grouped[group_key] = list()

        grouped[group_key].append(obj)

    return grouped