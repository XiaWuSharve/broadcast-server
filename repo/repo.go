package repo

import (
	"fmt"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	sts20150401 "github.com/alibabacloud-go/sts-20150401/v2/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type Client struct {
	client *tablestore.TableStoreClient
}

const (
	endPoint        = "https://localhost:8083"
	instanceName    = "x02caat39505"
	accessKeyId     = "<>"
	accessKeySecret = "<>"
)

func NewSyncRepo() *tablestore.TableStoreClient {
	// 从环境变量中获取步骤1.1生成的RAM用户的访问密钥（AccessKey ID和AccessKey Secret）。
	accessKeyId := "<>"
	accessKeySecret := "<>"
	// 从环境变量中获取步骤1.3生成的RAM角色的RamRoleArn。
	roleArn := "<>"

	// 创建权限策略客户端。
	config := &openapi.Config{
		// 必填，步骤1.1获取到的 AccessKey ID。
		AccessKeyId: tea.String(accessKeyId),
		// 必填，步骤1.1获取到的 AccessKey Secret。
		AccessKeySecret: tea.String(accessKeySecret),
	}
	// Endpoint 请参考 https://api.aliyun.com/product/Sts
	config.Endpoint = tea.String("sts.cn-hangzhou.aliyuncs.com")
	client, err := sts20150401.NewClient(config)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return nil
	}

	// 使用RAM用户的AccessKey ID和AccessKey Secret向STS申请临时访问凭证。
	request := &sts20150401.AssumeRoleRequest{
		// 指定STS临时访问凭证过期时间为3600秒。
		DurationSeconds: tea.Int64(3600),
		// 从环境变量中获取步骤1.3生成的RAM角色的RamRoleArn。
		RoleArn: tea.String(roleArn),
		// 指定自定义角色会话名称，这里使用和第一段代码一致的 examplename
		RoleSessionName: tea.String("go-ots-role"),
	}
	response, err := client.AssumeRoleWithOptions(request, &util.RuntimeOptions{})
	if err != nil {
		fmt.Printf("Failed to assume role: %v\n", err)
		return nil
	}

	// 打印STS返回的临时访问密钥（AccessKey ID和AccessKey Secret）、安全令牌（SecurityToken）以及临时访问凭证过期时间（Expiration）。
	credentials := response.Body.Credentials
	fmt.Println("AccessKeyId: " + tea.StringValue(credentials.AccessKeyId))
	fmt.Println("AccessKeySecret: " + tea.StringValue(credentials.AccessKeySecret))
	fmt.Println("SecurityToken: " + tea.StringValue(credentials.SecurityToken))
	fmt.Println("Expiration: " + tea.StringValue(credentials.Expiration))

	cfg := tablestore.NewDefaultTableStoreConfig()
	// config.ProxyHost =
	return tablestore.NewClientWithConfig(
		// endPoint string, instanceName string, accessKeyId string, accessKeySecret string, securityToken string, config *tablestore.TableStoreConfig, options ...tablestore.ClientOption
		endPoint,
		instanceName,
		tea.StringValue(credentials.AccessKeyId),
		tea.StringValue(credentials.AccessKeySecret),
		tea.StringValue(credentials.SecurityToken),
		cfg,
	)
}
