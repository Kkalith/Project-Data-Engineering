with 

payment_method as (

    select * from {{ ref('stg_hospital__payment_method') }}

),

renamed as (

    select *

    from payment_method

)

select * from renamed