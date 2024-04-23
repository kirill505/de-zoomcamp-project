import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    data_fixed = data.join(pd.json_normalize(data['node'])).drop('node', axis='columns')

    data_fixed = data_fixed.explode('topics.edges', ignore_index=True)
    data_fixed = data_fixed.join(pd.json_normalize(data_fixed['topics.edges'])).drop('topics.edges', axis='columns')
    data_fixed.rename(columns={'node.createdAt':'topic_createdAt', 'node.id': 'topic_id', 'node.name': 'topic_name' }, inplace=True)

    ph_posts_dtypes = {
        'cursor': str,
        'commentsCount': pd.Int64Dtype(),
        'description': str, 
        'id': pd.Int64Dtype(),
        'name': str,
        'slug': str,
        'tagline': str,
        'url': str,
        'userId': pd.Int64Dtype(), 
        'votesCount': pd.Int64Dtype(),
        'website': str,
        'topic_id': pd.Int64Dtype(), 
        'topic_name': str
    }

    parse_dates = ['createdAt', 'topic_createdAt']

    data_fixed = data_fixed.astype(ph_posts_dtypes)

    data_fixed['createdAt'] = pd.to_datetime(data_fixed['createdAt'])
    data_fixed['topic_createdAt'] = pd.to_datetime(data_fixed['topic_createdAt'])

    print(len(data_fixed))
    
    return data_fixed


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
