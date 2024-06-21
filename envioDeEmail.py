import os
from reportlab.platypus import Image as ReportLabImage
import mysql.connector
from reportlab.lib import utils
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Table, Paragraph
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import requests
from PIL import Image
import io
import textwrap
from reportlab.lib.pagesizes import letter
from PIL import Image
from reportlab.lib.units import inch

conexao = mysql.connector.connect(
    host='',
    user='',
    password='',
    database=''
)
cursor = conexao.cursor()

comando =   "SELECT ..."

cursor.execute(comando)
dataset = pd.DataFrame(cursor.fetchall())
novo_nome_colunas = ['Nome_Imagem','Data', 'Ref', 'Loja', 'Protocolo', 'Produto', 'Valor', 'Status', 'Sindico', 'Email']
dataset.columns = novo_nome_colunas
cursor.close()
conexao.close()

# if dataset.empty or dataset['Produto'].isna().all():
if dataset.empty:
    print("Estamos sem Infratores, então não vamos enviar e-mail para o síndico com o relatório do mês")
else:
    caminho_imagem = ''
    dataset = dataset.dropna(subset=['Valor'])
    grouped_data = dataset.groupby('Loja')

    def cabecalho_12meses(loja):
        conexao = mysql.connector.connect(
        host='',
        user='',
        password='',
        database=''
        )
        cursor = conexao.cursor()
        total_furtos= 'SELECT ...' 
        cursor.execute(total_furtos,(loja,))
        results = cursor.fetchall()

        dataset = pd.DataFrame(results, columns=['Valor', 'Status'])
        total_furtos = dataset['Valor'].sum()
        total_furtos = f'{total_furtos:.2f}'
        total_furtos = str(total_furtos).replace('.', ',')


        status_5 = dataset[dataset['Status'] == 5]
        total_recuperados = status_5['Valor'].sum()
        total_recuperados = f'{total_recuperados:.2f}'
        total_recuperados = str(total_recuperados).replace('.', ',')
        

        status_2 = dataset[dataset['Status'] == 3]
        total_nao_identificados = status_2['Valor'].sum()
        total_nao_identificados = f'{total_nao_identificados:.2f}'
        total_nao_identificados = str(total_nao_identificados).replace('.', ',')
        

        status_4 = dataset[dataset['Status'] == 4]
        total_em_recuperacao = status_4['Valor'].sum()
        total_em_recuperacao = f'{total_em_recuperacao:.2f}'
        total_em_recuperacao = str(total_em_recuperacao).replace('.', ',')
        cursor.close()
        conexao.close()
        

        return total_furtos, total_recuperados, total_nao_identificados, total_em_recuperacao
    
    def add_image_to_first_page(canvas, doc):
            canvas.saveState()
            nova_largura = 150
            imagem = utils.ImageReader(caminho_imagem)
            largura_original, altura_original = imagem.getSize()
            proporcao = nova_largura / largura_original
            nova_altura = altura_original * proporcao
            canvas.drawImage(caminho_imagem, 40, doc.height + 100 - nova_altura, width=nova_largura,
                              height=nova_altura, mask='auto')
            canvas.restoreState()
            print("Adicionando logo SmartBreak")
            
    def download_and_convert_image(nome_imagem):
        url_imagem = f""
        response = requests.get(url_imagem)
        if response.status_code == 200:
            print('Get realizado com sucesso!')
            imagem = Image.open(io.BytesIO(response.content))
            caminho_imagem1 = f"{nome_imagem}" 
            imagem.save(caminho_imagem1)
            return caminho_imagem1
        else:
            print(f"Falha ao baixar a imagem {nome_imagem}")
            return None

    print('Separando dados por loja')
    for loja, group_data in grouped_data:        
        imagens = []
        nomes_imagens = group_data['Nome_Imagem'].tolist()
        print('Get das imagens dos infratores')
        for nome_imagem in nomes_imagens:
            if nome_imagem in imagens:
                print('Já baixamos essa imagem')
            else:
                caminho_imagem1 = download_and_convert_image(nome_imagem)
            if caminho_imagem1:
                imagens.append(caminho_imagem1) 
        print('Tratando dados')
        if imagens:  
            group_data['Caminho_Imagem'] = imagens
        else:
            print(f"Não foram encontradas imagens para a loja {loja}")
        destinatario = str(group_data['Email'].iloc[0])
        cabecalho_12meses(loja)
        balanco_12meses = cabecalho_12meses(loja)
        total_furtos = balanco_12meses[0]
        total_recuperados = balanco_12meses[1]
        total_nao_identificados = balanco_12meses[2]
        total_em_recuperacao = balanco_12meses[3]
        
        total_sindico = group_data['Valor'].sum()
        group_data.loc[group_data['Status'] == 3, 'Status'] = 'Em identificação'
        group_data.loc[group_data['Status'] == 4, 'Status'] = 'Em cobrança'
        group_data['Data'] = pd.to_datetime(group_data['Data'])
        group_data['Data'] = group_data['Data'].dt.strftime('%d/%m/%Y %H:%M:%S')
        group_data['Valor'] = group_data['Valor'].astype(str).str.replace(".", ",")
        group_data['Produto'] = group_data['Produto'].apply(lambda x: '\n'.join(textwrap.wrap(x, width=20)))
        new_df = group_data.drop(columns=['Nome_Imagem','Sindico', 'Email'])
        tables = [new_df.iloc[i:i + 30] for i in range(0, len(new_df), 30)]
        loja = group_data['Loja'].iloc[0]
        print(group_data)
        print(f'Criando PDF da loja {loja}')

        pdf_filename = f'{loja}.pdf'
        doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
        styles = getSampleStyleSheet()
        style = styles["BodyText"]
        elements = []
        doc.build([Paragraph("", style)], onFirstPage=add_image_to_first_page)
        total_sindico = f'{total_sindico:.2f}'.replace('.', ',')
        texto = f"Notificação de pagamentos não identificados: {loja}."
        texto1 = f"Valor total das ocorrências: R${total_sindico}."
        texto2 = f"Total furtos: R${total_furtos}."
        texto3 = f"Total recuperados: R${total_recuperados}."
        texto4 = f"Total nao identificados: R${total_nao_identificados}."
        texto5 = f"Total em recuperacao: R${total_em_recuperacao}."
        texto8 = "Privacidade: Este documento é confidencial e destina-se exclusivamente a comunicação entre Smart Break e administradores."
        texto9 = "Não é autorizada a divulgação e o compartilhamento dos dados aqui informados, exceto aos envolvidos citados."
        texto10 = " "
        texto11 = " "
        texto12 = " "
        elements.append(Paragraph(texto, style))
        elements.append(Paragraph(texto1, style))
        elements.append(Paragraph(texto2, style))
        elements.append(Paragraph(texto3, style))
        elements.append(Paragraph(texto4, style))
        elements.append(Paragraph(texto5, style))
        elements.append(Paragraph(texto8, style))
        elements.append(Paragraph(texto9, style))
        elements.append(Paragraph(texto10, style))
        elements.append(Paragraph(texto11, style))
        elements.append(Paragraph(texto12, style))

        laco = 0

        def add_table_to_pdf(elements, table, imagens, loja, laco):
            for index, row in table.iterrows():
                if pd.isna(row['Ref']):  
                    table.at[index, 'Ref'] = 0
            
            if i > 0:
                elements.append(Paragraph('<br/><br/><br/>', style))
            
            grouped_data = table.groupby('Ref')
            for protocolo, group_data in grouped_data:
                group_data['Valor'] = group_data['Valor'].str.replace(',', '.')
                group_data['Valor'] = group_data['Valor'].astype(float)
                total_infrator = group_data['Valor'].sum()
                group_data['Valor'] = group_data['Valor'].map('R${:,.2f}'.format)
                group_data['Valor'] = group_data['Valor'].astype(str)
                group_data['Valor'] = group_data['Valor'].str.replace('.', ',')
                total_infrator = f'{total_infrator:.2f}'
                total_infrator = str(total_infrator).replace('.', ',')
                num_colunas = table.shape[1]
                if num_colunas == 8:
                    for index, row in group_data.iterrows():
                        if imagens:  
                            caminho_imagem1 = row['Caminho_Imagem']
                        else:
                            print(f"Não foram encontradas imagens para a loja {loja}")

                        if caminho_imagem1:
                            group_data.at[index, 'Imagem'] = ReportLabImage(caminho_imagem1, width=1*inch, height=1*inch) 
        
                    if imagens:  
                        group_data = group_data.drop(columns=['Caminho_Imagem'])
                    else:
                        print(f"Não foram encontradas imagens para a loja {loja}")

                    group_data = group_data.drop(columns=['Ref'])
                    table_data = [group_data.columns.tolist()] + group_data.values.tolist()
                    additional_row = [' ', ' ', ' ', 'Total', f'R${total_infrator}',' ', ' ']
                    table_data.append(additional_row)
                    table_data[2][-1] = '  Identificado'
                    for sublist in table_data[3:]:
                        del sublist[-1]
                    
                    
                    if laco == 0 :
                        laco += 1
                        t = Table(table_data, style=[
                            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('LINEBEFORE', (0,0), (0,-1), 1, colors.black),  
                            ('LINEAFTER', (-1,0), (-1,-1), 1, colors.black), 
                            ('LINEABOVE', (0,0), (-1,0), 1, colors.black),  
                            ('LINEBELOW', (0,-1), (-1,-1), 1, colors.black),  
                        ])
                        column_widths = [1.5*inch, 1.4*inch, 1*inch, 1.5*inch, 1*inch, 0.5*inch, 1*inch] 

                        t._argW = column_widths          
                        
                        elements.append(t)
                    else:
                        
                        laco += 1
                        table_data = table_data[1:]

                        t = Table(table_data, style=[
                            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('LINEBEFORE', (0,0), (0,-1), 1, colors.black),  
                            ('LINEAFTER', (-1,0), (-1,-1), 1, colors.black), 
                            ('LINEABOVE', (0,0), (-1,0), 1, colors.black),  
                            ('LINEBELOW', (0,-1), (-1,-1), 1, colors.black),  
                        ])

                        
                        column_widths = [1.5*inch, 1.4*inch, 1*inch, 1.5*inch, 1*inch, 0.5*inch, 1*inch, ]
                        t._argW = column_widths 

                        elements.append(t)
                else:
                    if imagens:  
                        group_data = group_data.drop(columns=['Caminho_Imagem'])
                    else:
                        print(f"Não foram encontradas imagens para a loja {loja}")
                    group_data = group_data.drop(columns=['Ref'])
                    table_data = [group_data.columns.tolist()] + group_data.values.tolist()
                    for data in table_data[1:2]:
                        table_data[1].append('  Não identificado')
                    additional_row = [' ', ' ', ' ', 'Total', f'R${total_infrator}',' ']
                    table_data.append(additional_row)
                    if laco == 0 :
                        laco += 1
                        t = Table(table_data, style=[
                            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('LINEBEFORE', (0,0), (0,-1), 1, colors.black),  
                            ('LINEAFTER', (-1,0), (-1,-1), 1, colors.black), 
                            ('LINEABOVE', (0,0), (-1,0), 1, colors.black),  
                            ('LINEBELOW', (0,-1), (-1,-1), 1, colors.black),  
                        ])
                        column_widths = [1.5*inch, 1.4*inch, 0.5*inch, 1.5*inch, 1*inch, 0.5*inch, 1*inch] 

                        t._argW = column_widths          
                        
                        elements.append(t)
                    else:
                        
                        laco += 1
                        table_data = table_data[1:]

                        t = Table(table_data, style=[
                            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('LINEBEFORE', (0,0), (0,-1), 1, colors.black),  
                            ('LINEAFTER', (-1,0), (-1,-1), 1, colors.black), 
                            ('LINEABOVE', (0,0), (-1,0), 1, colors.black),  
                            ('LINEBELOW', (0,-1), (-1,-1), 1, colors.black),  
                        ])
    
                        
                        column_widths = [1.5*inch, 1.4*inch, 0.5*inch, 1.5*inch, 1*inch, 0.5*inch, 1*inch]
                        t._argW = column_widths 

                        elements.append(t)
            return laco

        for i, table in enumerate(tables):
            laco = add_table_to_pdf(elements, table, imagens, loja, laco)

        doc.build(elements)
        print(f'PDF criado com sucesso para a loja {loja}: {pdf_filename}')

        
        def enviar_email(destinatario, pdf_filename, loja):
            smtp_server = ''
            porta = 587  
            remetente = '' 
            senha = ''
            assunto = f"Consolidado do condominio {loja}"
            
            msg = MIMEMultipart()
            msg['From'] = remetente
            msg['To'] = destinatario
            msg['Subject'] = assunto
            
            mensagem = '''\
                Olá, como vai?

                Meu nome é João Victor, sou analista de prevenção e perdas aqui na Smart Break!
                Sou o responsável por tratar dos assuntos de incidentes de transação e prevenção de novas ocorrências.

                O que é um incidente de transação?
                Incidente de transação é quando de alguma forma deixamos de receber o valor da compra, seja por erro no pagamento em nossos canais (totem e app), retirada de itens sem efetuar o pagamento e diversos outros acontecimentos que podem gerar os incidentes.

                Estou encaminhando o nosso relatório de incidentes que foram gerados no Setin Downtown, para análise dos responsáveis.

                A nossa pretensão com os pagamentos é que possamos melhorar cada vez mais a experiência do cliente em nossos minimercados, e que com o aumento do faturamento através desses pagamentos não reconhecidos possamos aplicar melhorias na infraestrutura e conseguir nos aproximar cada vez mais do nosso consumidor final com ativações, promoções e mais ações relevantes que possam gerar uma interação positiva para todos.

                Estou à disposição para esclarecimentos, aliás, o que acha de agendarmos um bate-papo?
                '''
            msg.attach(MIMEText(mensagem, 'plain'))

            anexo_path = pdf_filename
            with open(anexo_path, 'rb') as anexo:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(anexo.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename= {anexo_path}')
            msg.attach(part)

            try:
                server = smtplib.SMTP(smtp_server, porta)
                server.starttls()
                server.login(remetente, senha)
                texto = msg.as_string()
                server.sendmail(remetente, destinatario, texto)
                print(f"Email enviado com sucesso para {destinatario}, com o anexo {anexo_path}")
            except Exception as e:
                print(f"Erro ao enviar email para {destinatario}: {e}")
            finally:
                server.quit()
        
        # Chamada da função para enviar o email
        destinatarioteste = ""
        # destinatarioteste = ""
        enviar_email(destinatarioteste, pdf_filename, loja)



    arquivos = os.listdir('')
    for arq in arquivos:
        if '143750.png' not in arq and arq.endswith('.png'):
            os.remove(arq)
        if arq.endswith('.pdf'):
            os.remove(arq)