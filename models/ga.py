import abc
from typing import List


class Property:

    def __init__(self, name, property_id, metrics, dimensions, dwh_columns, dwh_table):
        self.name = name
        self.property_id = property_id
        self.metrics = metrics
        self.dimensions = dimensions
        self.dwh_columns = dwh_columns
        self.dwh_table = dwh_table


class PDFForge(Property):
    def __init__(self):
        super().__init__(
            name='pdfforge-GA4',
            property_id='318694191',
            metrics=["sessions", "totalUsers", "newUsers"],
            dimensions=[
                "date",
                "country",
                "landingPage",
                "sessionDefaultChannelGrouping",
                "hostName",
                "eventName",
                "sessionCampaignName",
                ],
            dwh_table='GA.Sessions_Architect',
            dwh_columns=[
                "[Date]",
                "[Country]",
                "[LandingPagePath]",
                "[ChannelGrouping]",
                "[HostName]",
                "[EventName]",
                "[SessionCampaign]",
                "[Sessions]",
                "[Users]",
                "[NewUsers]",
                ]
        )


class RullupPDF(Property):
    def __init__(self):
        super().__init__(
            name='rullup pdf business',
            property_id='313884864',
            metrics=["sessions", "totalUsers", "newUsers"],
            dimensions=[
                "date",
                "country",
                "landingPage",
                "sessionDefaultChannelGrouping",
                "hostName",
                "streamId",
                "streamName",
                "customUser:website_id",
                "customUser:affiliate_id"
                ],
            dwh_table='GA.Sessions_PDF',
            dwh_columns=[
                "[Date]",
                "[Country]",
                "[LandingPagePath]",
                "[ChannelGrouping]",
                "[HostName]",
                "[StreamID]",
                "[StreamName]",
                "[WebSiteID]",
                "[AffiliateID]",
                "[Sessions]",
                "[Users]",
                "[NewUsers]",
            ]
        )


class Inpixio(Property):
    def __init__(self):
        super().__init__(
            name='Inpixio',
            property_id='252971573',
            metrics=["sessions", "totalUsers", "newUsers"],
            dimensions=[
                "date",
                "country",
                "landingPage",
                "sessionDefaultChannelGrouping",
                "hostName",
                "sessionCampaignName",
                "eventName"
                ],

            dwh_table='[GA].[Sessions_InPixio]',
            dwh_columns=[
                "[Date]",
                "[Country]",
                "[LandingPagePath]",
                "[ChannelGrouping]",
                "[HostName]",
                "[SessionCampaign]",
                "[EventName]",
                "[Sessions]",
                "[Users]",
                "[NewUsers]",
                ]
        )


class PCH(Property):
    def __init__(self):
        super().__init__(
            name='PCH',
            property_id='287180673',
            metrics=["sessions", "totalUsers", "newUsers"],
            dimensions=[
                "date",
                "country",
                "landingPage",
                "sessionDefaultChannelGrouping",
                "hostName",
                "sessionCampaignName",
                "eventName"
                ],

            dwh_table='[GA].[Sessions_PCH]',
            dwh_columns=[
                "[Date]",
                "[Country]",
                "[LandingPagePath]",
                "[ChannelGrouping]",
                "[HostName]",
                "[SessionCampaign]",
                "[EventName]",
                "[Sessions]",
                "[Users]",
                "[NewUsers]",
                ]
        )
class GoogleAnalytic(abc.ABC):

    def __init__(self, name, properties: List[Property], default_start_date='2023-01-01'):
        self.name = name
        self.properties = properties
        self.default_start_date = default_start_date



class SodaPDFAccount(GoogleAnalytic):
    def __init__(self):
        rullup = RullupPDF()
        super().__init__(
            name='Soda Pdf',
            properties=[
                rullup
            ]
        )


class PDFForgeAccount(GoogleAnalytic):
    def __init__(self):
        pdfforge = PDFForge()
        super().__init__(
            name='Soda Pdf',
            properties=[
                pdfforge
            ]
        )


class InpixioAccount(GoogleAnalytic):
    def __init__(self):
        super().__init__(
            name='Inpixio',
            properties=[
                Inpixio()
            ], 
            default_start_date='2022-01-01'
        )
        

class PCHAccount(GoogleAnalytic):
    def __init__(self):
        super().__init__(
            name='PCH',
            properties=[
                PCH()
            ], 
            default_start_date='2023-01-01'
        )