with ads as (SELECT distinct BIGQ.DOMAIN domain_url, 
                BIGQ.SSP as SSP, 
                BIGQ.ACCOUNTS as authorized_account, 
                case when BIGQ.SSP = '3P' then 'Null' else BIGQ.relationship_type end as Relationship_Type
                

FROM(
SELECT  f.publisher_domain DOMAIN,
                CASE x.ad_system_domain
                    WHEN 'nexage.com' THEN 'One by AOL: Mobile'
                    WHEN 'aol.com' THEN 'One by AOL: Mobile'
                    WHEN 'adtech.com' THEN 'One by AOL: Display'
                    WHEN 'aolcloud.net' THEN 'One by AOL: Display'
                    WHEN 'adap.tv' THEN 'One by AOL: Video'
                    WHEN 'advertising.com' THEN 'One by AOL: Video'
                    ELSE '3P'
                END as SSP,
          case when x.ad_system_domain in('nexage.com', 'aol.com', 'adtech.com', 'aolcloud.net', 'adap.tv', 'advertising.com') then  x.account_id else '123456789' end AS ACCOUNTS,                    
          x.relationship_type relationship_type
   FROM
     (SELECT t.publisher_domain,
             t.state,
             t.sellers,
             t.dt
      FROM
        (SELECT publisher_domain,
                max(dt) mdt
         FROM tns_ils.seller_verification
         WHERE dt > '20190201'
         GROUP BY publisher_domain
         ) u
      INNER JOIN
        (SELECT *
         FROM tns_ils.seller_verification
         WHERE dt > '20190201') t ON t.dt = u.mdt
      -- HIVE--AND t.publisher_domain = u.publisher_domain) f LATERAL VIEW explode(f.sellers) exploded_table AS x
      AND t.publisher_domain = u.publisher_domain) f CROSS JOIN UNNEST(f.sellers)  AS x (x)

   GROUP BY f.publisher_domain,
            x.ad_system_domain,
            x.account_id,
            x.relationship_type) BIGQ)

SELECT
       one_video.site_url,
       one_video.device,
       one_video.managing_organization_id,
       one_video.company_name,
       COALESCE(one_video.ssp, COALESCE(link.Flag,'Uncategorized')), 
       one_video.revenue,
       one_video.Market_Ops,
       one_video.Ad_Ops
FROM(
select dayy.site_url site_url,
       dayy.device,
       dayy.managing_organization_id,
       org.company_name,
       ads.ssp,
       dayy.revenue,
       dayy.Market_Ops,
       dayy.Ad_Ops


FROM

(select 
case when page_url is null then mobile_app_name when lower(page_url) like '%yahoo%' then 'yahoo.com'  else lower(page_url) end "site_url",
case when video_platform_id = 0 then 'Online Video' when video_platform_id in(1, 6) and inventory_type = 1 then 'Mobile Web' when video_platform_id in(1, 6) and inventory_type = 2 then 'CTV or InApp Supply' when video_platform_id = 11 then 'CTV or InApp Supply' when video_platform_id = 12 then 'CTV or InApp Supply' else 'Other' end device,
supply_local_supply_seat_id "managing_organization_id",
sum(supply_publisher_revenue) / 1E9 "revenue",
sum(seller_market_opportunities) "Market_Ops",
sum(ad_opportunities) "Ad_Ops"
FROM macaw_prod.auction_analytics
where placement_type = 'MARKET'
and datestamp >= replace(cast(current_date - interval '7' day as varchar), '-')
group by 
case when page_url is null then mobile_app_name when lower(page_url) like '%yahoo%' then 'yahoo.com'  else lower(page_url) end,
case when video_platform_id = 0 then 'Online Video' when video_platform_id in(1, 6) and inventory_type = 1 then 'Mobile Web' when video_platform_id in(1, 6) and inventory_type = 2 then 'CTV or InApp Supply' when video_platform_id = 11 then 'CTV or InApp Supply' when video_platform_id = 12 then 'CTV or InApp Supply' else 'Other' end,
supply_local_supply_seat_id) DAYY

JOIN (select distinct id,company_name,customer_type,sales_director from macaw_prod.dim_organizations where account_type != 'TEST') org on (dayy.managing_organization_id = org.id) 
--LEFT JOIN (select domain_url, ssp as one_by_aol_video, authorized_account from ads ) adss on (ads.domain_url = dayy.site_url and dayy.managing_organization_id = ads.authorized_account)
LEFT join ads on (ads.domain_url = dayy.site_url and cast(dayy.managing_organization_id as varchar) = ads.authorized_account) 
) one_video
LEFT OUTER JOIN  (select distinct lower(domain_url) domain_url,'Unauthorized' as "Flag" from ads) link on (one_video.site_url = link.domain_url)