with 

src_cancel_reason as (

    select cancel_reason from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        md5(cancel_reason) as id_cancel_reason,
        cancel_reason

    from src_cancel_reason

)

select * from renamed