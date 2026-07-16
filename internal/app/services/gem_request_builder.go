package services

import "time"

type GemRequestBuilder struct {
	request *GemReportRequest
}

type GemExceptionRequestBuilder struct {
	GemRequestBuilder
}

type GemTransactorRequestBuilder struct {
	GemRequestBuilder
}

type GemDetailsRequestBuilder struct {
	GemRequestBuilder
}

func NewGemRequestBuilder(correlationID string) *GemRequestBuilder {
	return &GemRequestBuilder{
		request: &GemReportRequest{
			CorrelationID: correlationID,
		},
	}
}

func (b *GemRequestBuilder) WithException() *GemExceptionRequestBuilder {
	return &GemExceptionRequestBuilder{*b}
}

func (b *GemRequestBuilder) WithTransactor() *GemTransactorRequestBuilder {
	return &GemTransactorRequestBuilder{*b}
}

func (b *GemRequestBuilder) WithDetails() *GemDetailsRequestBuilder {
	return &GemDetailsRequestBuilder{*b}
}

func (b *GemRequestBuilder) Build() *GemReportRequest {
	return b.request
}

func (b *GemExceptionRequestBuilder) TypeName(typeName string) *GemExceptionRequestBuilder {
	b.request.ExceptionTypeName = typeName
	return b
}

func (b *GemExceptionRequestBuilder) SeverityName(severityName string) *GemExceptionRequestBuilder {
	b.request.ExceptionSeverityName = severityName
	return b
}

func (b *GemExceptionRequestBuilder) Name(name ExceptionNameEnum) *GemExceptionRequestBuilder {
	b.request.ExceptionName = name
	return b
}

func (b *GemExceptionRequestBuilder) Message(message string) *GemExceptionRequestBuilder {
	b.request.ExceptionMessage = message
	return b
}

func (b *GemExceptionRequestBuilder) ActualDate(date time.Time) *GemExceptionRequestBuilder {
	b.request.ActualExceptionDateTime = pacificLogTimestamp(date)
	return b
}

func (b *GemExceptionRequestBuilder) Source(source string) *GemExceptionRequestBuilder {
	b.request.DataSource = source
	return b
}

func (b *GemExceptionRequestBuilder) Target(target string) *GemExceptionRequestBuilder {
	b.request.DataTarget = target
	return b
}

func (b *GemExceptionRequestBuilder) Workflow(workflow string) *GemExceptionRequestBuilder {
	b.request.BusinessWorkflowName = workflow
	return b
}

func (b *GemTransactorRequestBuilder) Service(service string) *GemTransactorRequestBuilder {
	b.request.TransactorServiceName = service
	return b
}

func (b *GemTransactorRequestBuilder) Version(version string) *GemTransactorRequestBuilder {
	b.request.TransactorServiceVersion = version
	return b
}

func (b *GemTransactorRequestBuilder) Function(function string) *GemTransactorRequestBuilder {
	b.request.TransactorFunctionName = function
	return b
}

func (b *GemTransactorRequestBuilder) User(user string) *GemTransactorRequestBuilder {
	b.request.TransactorUserName = user
	return b
}

func (b *GemTransactorRequestBuilder) Computer(computer string) *GemTransactorRequestBuilder {
	b.request.TransactorComputerName = computer
	return b
}

func (b *GemDetailsRequestBuilder) StackTrace(trace string) *GemDetailsRequestBuilder {
	b.request.StackTrace = trace
	return b
}

func (b *GemDetailsRequestBuilder) Payload(payload string) *GemDetailsRequestBuilder {
	b.request.ProcessingPayload = payload
	return b
}
