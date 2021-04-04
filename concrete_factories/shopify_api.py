import time

import shopify
from core.config import logger
from core.generic_credential import GenericCredential
from models.shopify import ShopifyMetadata

from ..connector_factory import ConnectorFactory
from ..decorators import mapping_alias


@mapping_alias(["shopify"])
class ShopifyFactory(ConnectorFactory):

    API_VERSION = "2020-04"

    def __init__(self, credential: GenericCredential, date_range, **kwargs):
        self.connector_id = credential.id
        self.url = credential.uri
        self.platform = credential.platform
        self.company_id = credential.company_id
        self.metadata = credential.metadata
        self.rfc = credential.rfc

        self.custom_dates = date_range

        token = ShopifyMetadata.handle_metadata(
            self.company_id, self.connector_id, self.metadata, self.rfc
        )

        # if credentials are empty then
        # not even try to connect with the store
        if token:
            self.store_name = token.get("store_name")
            self.password = token.get("password")
            self.store_token = token.get("api_key")

            self.set_up()
            self.data = self.extract()

    def set_up(self):
        try:
            shop_url = "https://%s:%s@%s.myshopify.com/admin/api/%s" % (
                self.store_token,
                self.password,
                self.store_name,
                self.API_VERSION,
            )
            shopify.ShopifyResource.set_site(shop_url)
            logger.info("successful setup for {}".format(self.company_id))
            return True
        except Exception as e:
            logger.error(
                e,
                extra={
                    "store_name": self.store_name,
                    "client_id": self.company_id,
                },
            )

    def extract(self):
        logger.info("starting extraction for {} ---->".format(self.company_id))

        for dates in self.custom_dates:
            time.sleep(1)
            logger.info(
                f"Downloading data from {dates['from']} to {dates['to']}"
            )
            created_to = dates["to"]
            created_from = dates["from"]

            list_orders = []
            next_page = True
            payload = {
                "source_name": self.platform,
                "data": {"orders": list_orders},
                "client_id": self.company_id,
                "store_name": self.store_name,
            }
            if self.connector_id is not None:
                payload.update({"internal_id": self.connector_id})

            try:
                orders = shopify.Order.find(
                    created_at_min=created_from,
                    created_at_max=created_to,
                    status="any",
                    limit=250,
                )
                while next_page is not False:
                    formatted_orders = list(
                        map(lambda order: order.to_dict(), orders)
                    )
                    list_orders += formatted_orders

                    if not list_orders:
                        payload["data"] = ""

                    all_ok = self.send(payload)
                    if all_ok:
                        logger.info(
                            "successfully extracted {} orders".format(
                                len(list_orders)
                            )
                        )

                    next_page = orders.has_next_page()
                    orders = orders.next_page() if next_page else None
                    list_orders.clear()
                    time.sleep(3)
                time.sleep(2)

            except Exception as e:
                logger.error(
                    e,
                    extra={
                        "store_name": self.store_name,
                        "client_id": self.company_id,
                    },
                )
