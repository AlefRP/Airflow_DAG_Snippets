import os
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class DAGGenerator:
    def __init__(self, template_path: str, output_dir: str, partners: dict):
        """
        Inicializa o gerador de DAGs.

        Args:
            template_path (str): Caminho do arquivo de template.
            output_dir (str): Diretório de saída para os arquivos gerados.
            partners (dict): Configuração dos parceiros (schedule, path).
        """
        self.template_path = Path(template_path)
        self.output_dir = Path(output_dir)
        self.partners = partners

        # Certifique-se de que o diretório de saída existe
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_template(self) -> str:
        """
        Carrega o conteúdo do arquivo de template.

        Returns:
            str: Conteúdo do template.
        """
        if not self.template_path.is_file():
            logging.error(f"Template file not found: {self.template_path}")
            raise FileNotFoundError(f"Template file not found: {self.template_path}")

        with self.template_path.open("r") as template_file:
            logging.info(f"Template loaded from {self.template_path}")
            return template_file.read()

    def generate_dag_content(self, template: str, dag_id: str, schedule: str, data_path: str) -> str:
        """
        Substitui os placeholders no template com valores reais.

        Args:
            template (str): Conteúdo do template.
            dag_id (str): ID do DAG.
            schedule (str): Intervalo de agendamento.
            data_path (str): Caminho de dados.

        Returns:
            str: Conteúdo do arquivo DAG gerado.
        """
        return (
            template.replace("{{DAG_ID}}", dag_id)
                    .replace("{{SCHEDULE_INTERVAL}}", schedule)
                    .replace("{{DATA_PATH}}", data_path)
        )

    def write_dag_file(self, dag_id: str, content: str):
        """
        Escreve o conteúdo gerado para um arquivo Python.

        Args:
            dag_id (str): ID do DAG.
            content (str): Conteúdo do DAG.
        """
        output_file = self.output_dir / f"{dag_id}.py"
        with output_file.open("w") as file:
            file.write(content)
        logging.info(f"DAG file generated: {output_file}")

    def generate_all_dags(self):
        """
        Gera os DAGs para todos os parceiros.
        """
        template = self.load_template()

        for partner, details in self.partners.items():
            dag_id = f"dag_{partner}"
            schedule = details.get("schedule", "@daily")
            data_path = details.get("path", "/data/unknown")

            logging.info(f"Generating DAG: {dag_id} with schedule: {schedule}, path: {data_path}")
            dag_content = self.generate_dag_content(template, dag_id, schedule, data_path)
            self.write_dag_file(dag_id, dag_content)


if __name__ == "__main__":
    # Configuração
    TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "../dags/templates/dag_template.py")
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../dags/generated_dags")
    PARTNERS = {
        'snowflake': {
            'schedule': '@daily',
            'path': '/data/snowflake',
        },
        'netflix': {
            'schedule': '@weekly',
            'path': '/data/netflix',
        },
    }

    # Gerar os DAGs
    generator = DAGGenerator(template_path=TEMPLATE_PATH, output_dir=OUTPUT_DIR, partners=PARTNERS)
    generator.generate_all_dags()
