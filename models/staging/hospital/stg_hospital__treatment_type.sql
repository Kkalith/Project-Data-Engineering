with 

src_treatment_type as (

    select treatment_type from {{ source('hospital', 'treatments') }}

),

renamed as (

    select distinct
        md5(treatment_type) as id_treatment_type,
        treatment_type

    from src_treatment_type

)

select * from renamed