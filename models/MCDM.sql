with
    data_model as (
        select
            __insert_date,
            ad_id,
            add_to_cart,
            adset_id,
            campaign_id,
            channel,
            clicks,
            comments,
            creative_id,
            creative_title,
            objective,
            buying_type,
            campaign_type,
            creative_body,
            date,
            likes,
            shares,
            comments_2,
            views,
            clicks_2,
            impressions,
            mobile_app_install,
            inline_link_clicks,
            purchase,
            complete_registration,
            purchase_value,
            shares_2,
            spend,
            purchase_2,
            views_2
        from {{ ref("src_ads_creative_facebook_all_data") }}
        union all
        select
            __insert_date,
            ad_id,
            null as add_to_cart,
            adset_id,
            campaign_id,
            channel,
            clicks,
            null as comments,
            null as creative_id,
            null as creative_title,
            null as objective,
            null as buying_type,
            null as campaign_type,
            null as creative_body,
            date,
            null as likes,
            null as shares,
            null as comments_2,
            null as views,
            null as clicks_2,
            null as impressions,
            null as mobile_app_install,
            null as inline_link_clicks,
            conv as purchase,
            null as complete_registration,
            null as purchase_value,
            null as shares_2,
            spend,
            null as purchase_2,
            null as views_2
        from {{ ref("src_ads_bing_all_data") }}
        union all
        select
            __insert_date,
            ad_id,
            add_to_cart,
            null as adset_id,
            campaign_id,
            channel,
            clicks,
            null as comments,
            null as creative_id,
            null as creative_title,
            null as objective,
            null as buying_type,
            null as campaign_type,
            null as creative_body,
            date,
            null as likes,
            null as shares,
            null as comments_2,
            null as views,
            null as clicks_2,
            impressions,
            null as mobile_app_install,
            null as inline_link_clicks,
            conversions as purchase,
            null as complete_registration,
            null as purchase_value,
            null as shares_2,
            spend,
            null as purchase_2,
            null as views_2
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        union all
        select
            __insert_date,
            null as ad_id,
            null as add_to_cart,
            null as adset_id,
            campaign_id,
            channel,
            clicks,
            comments,
            null as creative_id,
            null as creative_title,
            null as objective,
            null as buying_type,
            null as campaign_type,
            null as creative_body,
            date,
            likes,
            null as shares,
            null as comments_2,
            null as views,
            null as clicks_2,
            impressions,
            null as mobile_app_install,
            null as inline_link_clicks,
            null as purchase,
            null as complete_registration,
            null as purchase_value,
            null as shares_2,
            spend,
            null as purchase_2,
            null as views_2
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    ),

    conversion_cost as (
        select
            data_model.channel,
            round(
                sum(data_model.spend) / sum(data_model.purchase), 0
            ) as conversion_cost_by_channel
        from data_model
        group by data_model.channel
        order by 2 desc
    ),

    impressions_by_channel as (
        select data_model.channel, sum(data_model.impressions) as impressions_by_channel
        from data_model
        group by data_model.channel
        order by 2 desc
    ),

    cost_per_engage as (
        select
            data_model.channel,
            round(
                case
                    when data_model.channel = 'Facebook'
                    then
                        sum(data_model.spend) / sum(
                            data_model.comments
                            + data_model.shares
                            + data_model.views
                            + data_model.mobile_app_install
                            + data_model.inline_link_clicks
                        )
                    when data_model.channel = 'Twitter'
                    then sum(data_model.spend) / sum(tw.engagements)
                    else 0
                end,
                2
            ) as engagement_cost
        from data_model
        left join
            {{ ref("src_promoted_tweets_twitter_all_data") }} as tw
            on tw.__insert_date = data_model.__insert_date
            and tw.campaign_id = data_model.campaign_id
        group by data_model.channel

    ),

    cpc as (
        select channel, round(sum(spend) / sum(clicks), 2) as cpc
        from data_model
        group by channel
        order by 2 desc
    )

select
    cc.channel,
    conversion_cost_by_channel,
    ibc.impressions_by_channel,
    cpc.cpc,
    cpe.engagement_cost
from conversion_cost cc
join impressions_by_channel ibc on ibc.channel = cc.channel
join cpc on cpc.channel = cc.channel
join cost_per_engage as cpe on cpe.channel = cc.channel