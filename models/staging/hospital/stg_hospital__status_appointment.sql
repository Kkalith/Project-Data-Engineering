with 

src_status_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        md5(status) as id_status,
        status

    from src_status_appointments

)

select * from renamed