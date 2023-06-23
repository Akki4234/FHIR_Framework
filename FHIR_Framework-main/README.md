# FHIR Framework for Flattening JSON Files

Welcome to the FHIR Framework for Flattening JSON Files! 

This framework is designed to make it easy to flatten JSON files that conform to the [FHIR standard](https://www.hl7.org/fhir/) into a more accessible format. By flattening the JSON, you can simplify the data and make it easier to work with, whether you're analyzing the data, building a user interface, or integrating it into another system.

## How to use the Framework

### Steps to use FHIR framework:

- Add Input Json file as Data file in Input_Files folder
- Configure the config.json file in COnfig_files folder

### Step to create Config File:
- Create your pyspark schema and then dump it in json format and then Paste it  under Source Schema field.
- For Example: This is example input json file for healthcare data
# Example 1
```
{
	"resourceType": "HealthcareService",
	"id": "283-hs04HealthCareServiceNewPatientAcceptingNeg",
	"meta": {
		"versionId": "1",
		"lastUpdated": "2022-08-25T14:56:40.090-05:00",
		"profile": [
			"http://bpd.bcbs.com/StructureDefinition/bpd-healthcareservice-plan-submission",
			"http://bpd.bcbs.com/StructureDefinition/"
		],
		"Features":[
			"This is trial",
			"Just trying",
			"Testing replicacy"
		],
		"healthdata":[
			"health check",
			"This is only check"
			
		]
	},
}

```
## It's Source Schema:

```
      {
        "type": "struct",
        "fields": [
          {
            "name": "resourceType",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "id",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "meta",
            "type": {
              "type": "struct",
              "fields": [
                {
                  "name": "versionId",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "lastUpdated",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "profile",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "Features",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "healthdata",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                }
              ]
            },
            "nullable": true,
            "metadata": {}
          }
        }
 ```
 - Above schema consist some elements which are of Array Type and some elements are of struct type
 - All Elements Which are of Struct Type and are not Nested should Be called directly In struct_column_dict with key 1 and value will be array consiting af all elements which are nested at level 1
 - Similarly elements which are of type array and are nested in struct are might not be nested, but are array should be called in array_column_dict with key 1
 - Elemnts which are nested in struct should be called using ('.') dot notation and remember all ('.') will be replaced by ( _ ) during next iteration
 - Struct types which are nested at 2nd level should be included in struct_column_dict at with key 2. and value will be array which consist  nested structs at 2nd level

### Example to Generate struct_column_dict
```
{
        "1": [
            "resourceType",
            "id",
            "meta.versionId",
            "meta.lastUpdated",
            "meta.profile",
            "meta.Features",
            "meta.healthdata"
        ]
    }
```
### Similarly add array elements in array_column_dict with proper key values which matches the nested levels 
- Anything which is nested in some another element whould be called with ( _ ) attached to previous element for example
- see this.
```
{
      "1": [
          "meta_profile",
          "meta_Features",
          "meta_healthdata",
      ]
    }

```

# Example 2 (Lets  Make it more complicated)
```
{
	"resourceType": "HealthcareService",
	"id": "283-hs04HealthCareServiceNewPatientAcceptingNeg",
	"meta": {
		"versionId": "1",
		"lastUpdated": "2022-08-25T14:56:40.090-05:00",
		"profile": [
			"http://bpd.bcbs.com/StructureDefinition/bpd-healthcareservice-plan-submission",
			"http://bpd.bcbs.com/StructureDefinition/"
		],
		"Features":[
			"This is trial",
			"Just trying",
			"Testing replicacy"
		],
		"healthdata":[
			"health check",
			"This is only check"
			
		]
	},
	"extension" : [
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/geocodereturn",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "http://bpd.bcbs.com/CodeSystem/BCBSGeocodeReturnCodeCS",
				"code" : "Z",
				"display" : "Street level"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/servicearea",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "http://bpd.bcbs.com/CodeSystem/BCBSServiceAreaCS",
				"code" : "L",
				"display" : "Licensed"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/officelanguage",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "urn:ietf:bcp:47",
				"code" : "en",
				"display" : "English"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/officelanguage",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "urn:ietf:bcp:47",
				"code" : "it",
				"display" : "Italy"
			  }
			]
		  }
		}
	  ]
}

```
## It's Source Schema

```
{
        "type": "struct",
        "fields": [
          {
            "name": "resourceType",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "id",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "meta",
            "type": {
              "type": "struct",
              "fields": [
                {
                  "name": "versionId",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "lastUpdated",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "profile",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "Features",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "healthdata",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                }
              ]
            },
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "extension",
            "type": {
              "type": "array",
              "elementType": {
                "type": "struct",
                "fields": [
                  {
                    "name": "url",
                    "type": "string",
                    "nullable": true,
                    "metadata": {}
                  },
                  {
                    "name": "valueCodeableConcept",
                    "type": {
                      "type": "struct",
                      "fields": [
                        {
                          "name": "coding",
                          "type": {
                            "type": "array",
                            "elementType": {
                              "type": "struct",
                              "fields": [
                                {
                                  "name": "system",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                },
                                {
                                  "name": "code",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                },
                                {
                                  "name": "display",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                }
                              ]
                            },
                            "containsNull": true
                          },
                          "nullable": true,
                          "metadata": {}
                        }
                      ]
                    },
                    "nullable": true,
                    "metadata": {}
                  }
                ]
              },
              "containsNull": true
            },
            "nullable": true,
            "metadata": {}
          }
        ]
      }
 ```

### Example to Generate struct_column_dict
```
{
        "1": [
            "resourceType",
            "id",
            "meta.versionId",
            "meta.lastUpdated",
            "meta.profile",
            "meta.Features",
            "meta.healthdata"
            
        ],
        "2": [
            "extension.url",
            "extension.valueCodeableConcept.coding"
        ],
        "3": [
          "extension_valueCodeableConcept_coding.system",
          "extension_valueCodeableConcept_coding.code",
          "extension_valueCodeableConcept_coding.display"
      ]
    }
```
### Similarly add array elements in array_column_dict with proper key values which matches the nested levels 
- Anything which is nested in some another element whould be called with ( _ ) attached to previous element for example
- see this.
```
{
      "1": [
          "meta_profile",
          "meta_Features",
          "meta_healthdata",
          "extension"
      ],
      "2": [
          "extension_valueCodeableConcept_coding"
      ]
    }

```
- If you want more examples with more nested structures and more compplex json files then you can refer Config_files folder.

## Features

The FHIR Framework for Flattening JSON Files includes the following features:

- Easy-to-use codebase
- Customizable options for flattening the JSON data
- Support for FHIR standard data formats
- Comprehensive documentation and examples

## Contributing

We welcome contributions to the FHIR Framework for Flattening JSON Files! If you would like to contribute, please follow our [contribution guidelines](CONTRIBUTING.md) and submit a pull request.


