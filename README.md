# sfn-ng

Example Stack Config:
```ruby
require 'attribute_struct'

AttributeStruct.new do
  region 'eu-central-1'
  tags do
    Projekt 'tgw'
  end
  template 'out/tgw.json'
  
  parameters do
    env 'dev'
  end
  apply_stacks %w[
    DEV-VPC-EU
    us_east_1__DEV-VPC-USA
  ]
  
  apply_mappings do
    vpc_cidr_usa do
      region 'us-east-1'
      stack_name 'DEV-VPC-USA'
      output_name 'VpcCidr'
    end
    vpc_cidr_eu do
      region 'eu-central-1'
      stack_name 'DEV-VPC-EU'
      output_name 'VpcCidr'
    end
  end
end

```