import requests  
import vimap.api.v1 as vimap  
  
@vimap.fcn_worker  
def get_url(url, connection):  
    return connection.get(url).text  
  
inputs = (  
    'http://www.google.com/',  
    'http://www.yahoo.com/',  
)  
with vimap.fork(  
    get_url(  
        connection=vimap.post_fork_closable_arg(requests.Session)  
    ),  
    range(2)  
) as pool:  
    outputs = tuple(pool.imap(inputs))  
  
print outputs
