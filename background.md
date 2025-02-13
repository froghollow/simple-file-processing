## DUDE File Processing, v5

The **Dirt-cheap Universal Data ELT (DUDE) File Processing Framework** has its roots in DDMAP (formerly the DMS Universal Data Ecosystem(DUDE) ). DUDE is *Dirt-cheap* because it is *serverless*.   Built on 100% AWS native services, all you need is an AWS Account -- no servers, EC2 instances, containers, or software licenses to acquire, build, operate, and maintain.   The DUDE abides, at zero cost, awaiting file arrival or schedule to trigger *Extract, Load, and Transform (ELT)* processing.  (*ELT* refers to loading data into the data lake before transforming it, rather than *ETL* which refers to transforming data before loading it.)  You pay-as-you-go only the cost of execution, not for mostly-idle infrastructure.

DUDE is *Universal* in the sense that it is completely parameter-driven and in no way application-specific.   Parameters can be entered via the AWS Management Console, or supplied via CLI/SDK arguments.  Metadata can be used as the parameter source to achieve complete process automation.

AWS has made many improvements to its native services since we began developing DUDE in 2018.  At that time, Glue and Step Functions were not yet in FedRAMP scope, and Cloudwatch Events had not yet evolved into EventBridge.  Previous versions of DUDE File Processing evolved from its existing code base mainly to address functional requirements rather than to take advantage of service improvements.  In contrast, DUDEv5 is a major rewrite from scratch, updating selected portions from previous versions to run with new and improved services.

Several factors driving new version ...

Getting rid of NiFi/SFTP requires FP to handle ZIP extraction
Many recent improvements to Glue

Switching IDE to Jupyter 

Lambda works OK, but has some limitations
- storage limitation solved by EFS, but still 15 min max
- need to split large files (divide and conquer)

Want to use Parquet instead of GZip'd CSV
- more efficient
- better data types

Keep using Step Functions, and probably the Batch data in DynDB

Use Glue ETL jobs instead of Lambda
