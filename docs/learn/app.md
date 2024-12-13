# Creating an async app featuring multiple tasks
[//]: # (TODO: put ref to the app concept)
Let's recap, in the previous [section](tasks.md), we learnt how to:

- transform Python functions into asynchronous tasks
- register tasks with a name
- create tasks with arguments
- create task which can publish progress updates

## Full async app

If we put everything together, we can build a full async app, featuring multiple tasks:

```python
--8<--
hello_world_app.py:app
--8<--
```

## Next

[//]: # (TODO: put ref to the worker pool concept)
Perfect, we've built our by registering many async tasks, we need to run pool of async workers, which run these tasks when asked to do so ! 