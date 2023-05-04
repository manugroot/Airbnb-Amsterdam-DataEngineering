if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(listing_details, listing, **kwargs):
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
    # Specify your transformation logic here

    listing['list_id'] = listing.index

    listing_reviews_dim = listing[['list_id','number_of_reviews','last_review','reviews_per_month']]
    listing_reviews_dim.rename(columns={'list_id': 'listing_reviews_id'}, inplace=True)

    listing_owner_dim = listing[['list_id','host_name']]
    listing_owner_dim.rename(columns={'list_id': 'listing_owner_id','host_name': 'owner_name'}, inplace=True)

    listing_neighbourhood_dim = listing[['list_id','neighbourhood']]
    listing_neighbourhood_dim.rename(columns={'list_id': 'listing_neighbourhood_id','neighbourhood': 'listing_neighbourhood'}, inplace=True)

    listing_room_dim = listing[['list_id','room_type','availability_365','calculated_host_listings_count']]
    listing_room_dim.rename(columns={'list_id': 'listing_room_id','calculated_host_listings_count':'room_capacity'}, inplace=True)

    listing_location_dim = listing[['list_id','latitude','longitude']]
    listing_location_dim.rename(columns={'list_id': 'listing_location_id','latitude':'listing_latitude','longitude':'listing_longitude'}, inplace=True)

    listing_details['listing_detail_id'] = listing_details.index

    listing_detail_dim = listing_details[['listing_detail_id','name','listing_url','space','requires_license','cancellation_policy']]
    listing_detail_dim.rename(columns={'name': 'listing_name'}, inplace=True)


    fact_table = listing.merge(listing_reviews_dim, left_on='list_id', right_on='listing_reviews_id') \
             .merge(listing_owner_dim, left_on='list_id', right_on='listing_owner_id') \
             .merge(listing_neighbourhood_dim, left_on='list_id', right_on='listing_neighbourhood_id') \
             .merge(listing_room_dim, left_on='list_id', right_on='listing_room_id') \
             .merge(listing_location_dim, left_on='list_id', right_on='listing_location_id')\
             .merge(listing_detail_dim, left_on='list_id', right_on='listing_detail_id') \
             [['listing_reviews_id', 'listing_owner_id', 'listing_neighbourhood_id',
               'listing_room_id', 'listing_location_id', 'listing_detail_id', 'price', 'minimum_nights']]
    
    return {
        "listing_reviews_dim":listing_reviews_dim.to_dict(orient="dict"),
        "listing_owner_dim":listing_owner_dim.to_dict(orient="dict"),
        "listing_neighbourhood_dim":listing_neighbourhood_dim.to_dict(orient="dict"),
        "listing_room_dim":listing_room_dim.to_dict(orient="dict"),
        "listing_location_dim":listing_location_dim.to_dict(orient="dict"),
        "listing_detail_dim":listing_detail_dim.to_dict(orient="dict"),
        "fact_table":fact_table.to_dict(orient="dict"),
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
