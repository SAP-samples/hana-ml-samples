using CatalogService as service from '../../srv/hana-ml-cat-service';

annotate CatalogService.POINTS_OF_SALES with @(UI: {LineItem: [
    {Value: name},
    {Value: city}
]});