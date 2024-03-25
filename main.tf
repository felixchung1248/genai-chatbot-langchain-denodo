resource "helm_release" "langchain-chatbot-denodo" {
  name       = "langchain-chatbot-denodo"
  chart      = "/chart"
  namespace  = "langchain-chatbot-denodo-ns"
  create_namespace = true
}